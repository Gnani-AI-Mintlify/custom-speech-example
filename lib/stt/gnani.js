'use strict';

/**
 * lib/stt/gnani.js
 *
 * Cognigy Voice Gateway <-> Gnani Vachana STT adapter.
 *
 * Drop this file in lib/stt/ and add a 'gnani' case to lib/stt/index.js.
 * Set GNANI_API_KEY in your environment.
 * Optionally set GNANI_LANG_CODE to override the default fallback language (default: en-IN).
 *
 * ── Protocol: Cognigy → this handler ────────────────────────────────────────
 *   1. JSON  { type: "start",  language, sampleRateHz, encoding, interimResults, options }
 *   2. Binary LINEAR16 PCM frames at 8 kHz, arbitrary chunk sizes
 *   3. JSON  { type: "stop" }
 *
 * ── Protocol: this handler → Gnani ──────────────────────────────────────────
 *   Connect to wss://api.vachana.ai/stt/v3/stream with headers:
 *     x-api-key-id   : GNANI_API_KEY
 *     lang_code      : e.g. "hi-IN"
 *     x-sample-rate  : "8000"
 *   Send: binary PCM frames, exactly GNANI_FRAME_BYTES (1024) bytes each
 *   Receive JSON: { type: "connected" | "processing" | "transcript" | "error", ... }
 *
 * ── Protocol: this handler → Cognigy ────────────────────────────────────────
 *   { type: "transcription", is_final, alternatives: [{ transcript, confidence }], language }
 *   { type: "error", error }
 */

const WebSocket = require('ws');
const assert = require('assert');

const GNANI_WSS_URL = 'wss://api.vachana.ai/stt/v3/stream';

/**
 * Gnani requires exactly 1024 bytes per binary frame
 * (512 × 16-bit samples = 32 ms at 8 kHz).
 * Cognigy sends chunks of arbitrary size, so we buffer and slice.
 */
const GNANI_FRAME_BYTES = 1024;

/**
 * Supported Gnani lang_code values.
 * Cognigy sends BCP-47 tags (e.g. "hi-IN") which happen to be identical,
 * so no translation is needed for Indian languages.
 */
const SUPPORTED_LANG_CODES = new Set([
  'en-IN', 'hi-IN', 'ta-IN', 'te-IN', 'kn-IN',
  'ml-IN', 'mr-IN', 'bn-IN', 'gu-IN', 'pa-IN',
]);

const DEFAULT_LANG = process.env.GNANI_LANG_CODE || 'en-IN';

function resolveLanguage(cognigyLang) {
  if (cognigyLang && SUPPORTED_LANG_CODES.has(cognigyLang)) {
    return cognigyLang;
  }
  // e.g. Cognigy sends "en-US" — try swapping region to IN
  if (cognigyLang) {
    const base = cognigyLang.split('-')[0];       // "en"
    const indiaVariant = `${base}-IN`;            // "en-IN"
    if (SUPPORTED_LANG_CODES.has(indiaVariant)) {
      return indiaVariant;
    }
  }
  return DEFAULT_LANG;
}

/**
 * Flush socket.audioBuffer to Gnani in exact GNANI_FRAME_BYTES chunks.
 * Leaves any remainder back in socket.audioBuffer.
 */
function flushAudioBuffer(socket) {
  if (!socket.gnaniSocket || socket.gnaniSocket.readyState !== WebSocket.OPEN) return;

  let buf = Buffer.concat(socket.audioBuffer);
  socket.audioBuffer = [];

  while (buf.length >= GNANI_FRAME_BYTES) {
    socket.gnaniSocket.send(buf.slice(0, GNANI_FRAME_BYTES));
    buf = buf.slice(GNANI_FRAME_BYTES);
  }

  // Keep the partial remainder for the next push
  if (buf.length > 0) {
    socket.audioBuffer = [buf];
  }
}

/**
 * Flush and pad the final partial frame with silence before closing.
 */
function flushFinalFrame(socket) {
  if (!socket.gnaniSocket || socket.gnaniSocket.readyState !== WebSocket.OPEN) return;
  if (!socket.audioBuffer || socket.audioBuffer.length === 0) return;

  const remainder = Buffer.concat(socket.audioBuffer);
  socket.audioBuffer = [];

  if (remainder.length > 0) {
    const padded = Buffer.alloc(GNANI_FRAME_BYTES, 0);
    remainder.copy(padded);
    socket.gnaniSocket.send(padded);
  }
}

function terminateSocket(socket) {
  if (socket.gnaniSocket) {
    flushFinalFrame(socket);
    socket.gnaniSocket.close(1000, 'done');
    socket.gnaniSocket = null;
  }
}

// ── Main handler ─────────────────────────────────────────────────────────────

const transcribe = async (logger, socket, url) => {

  // Vachana API key — prefer query param (passed by demo/Cognigy per-connection),
  // fall back to env var for server-wide configuration.
  const urlObj   = new URL(url || '', 'http://localhost');
  const gnaniKey = urlObj.searchParams.get('gnani_key');

  if (!gnaniKey) {
    logger.error('gnani-stt: no Vachana API key — set GNANI_API_KEY env var or pass ?gnani_key=');
    socket.send(JSON.stringify({ type: 'error', error: 'Missing Vachana API key' }));
    socket.close();
    return;
  }

  socket.on('message', async (data, isBinary) => {
    try {
      // ── Binary frame: audio from Cognigy ───────────────────────────────────
      if (isBinary) {
        if (!socket.gnaniSocket) return; // start not yet received

        socket.audioBuffer.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
        flushAudioBuffer(socket);
        return;
      }

      // ── Text frame: control message from Cognigy ───────────────────────────
      const obj = JSON.parse(data.toString());
      logger.info({ obj }, 'gnani-stt: received JSON message from Cognigy Voice Gateway');

      if (obj.type === 'start') {
        assert.ok(!socket.gnaniSocket, 'Received start more than once on same connection');

        const { language, sampleRateHz } = obj;
        const langCode = resolveLanguage(language);

        socket.audioBuffer = [];
        socket.gnaniLangCode = langCode;

        logger.info({ langCode, sampleRateHz }, 'gnani-stt: opening upstream connection');

        const gnaniSocket = new WebSocket(GNANI_WSS_URL, {
          headers: {
            'x-api-key-id':  gnaniKey,
            'lang_code':     langCode,
            'x-sample-rate': String(sampleRateHz || 8000),
          },
        });

        gnaniSocket
          .on('open', () => {
            logger.info('gnani-stt: upstream socket opened');
            socket.gnaniSocket = gnaniSocket;
          })

          .on('message', (buffer) => {
            let msg;
            try {
              msg = JSON.parse(buffer.toString());
            } catch (e) {
              logger.warn({ raw: buffer.toString() }, 'gnani-stt: non-JSON from Gnani — ignoring');
              return;
            }

            switch (msg.type) {
              case 'connected':
                logger.info({ config: msg.config }, 'gnani-stt: Gnani confirmed connected');
                break;

              case 'processing':
                // VAD detected end-of-speech; transcription in progress — no action needed
                logger.debug('gnani-stt: Gnani processing speech segment');
                break;

              case 'transcript': {
                const transcript = (msg.text || '').trim();
                if (!transcript) break;

                logger.info(
                  { transcript, latency: msg.latency, segmentIndex: msg.segment_index },
                  'gnani-stt: transcript received'
                );

                socket.send(JSON.stringify({
                  type:         'transcription',
                  is_final:     true,
                  language:     socket.gnaniLangCode,
                  channel:      1,
                  alternatives: [{ transcript, confidence: 1.0 }],
                }));
                break;
              }

              case 'error':
                logger.error({ gnaniError: msg.message }, 'gnani-stt: error from Gnani');
                socket.send(JSON.stringify({ type: 'error', error: `Gnani STT error: ${msg.message}` }));
                break;

              default:
                logger.debug({ type: msg.type }, 'gnani-stt: unknown message type from Gnani — ignoring');
            }
          })

          .on('error', (err) => {
            logger.error({ err }, 'gnani-stt: upstream socket error');
            socket.send(JSON.stringify({ type: 'error', error: `Gnani connection error: ${err.message}` }));
          })

          .on('close', (code, reason) => {
            logger.info({ code, reason: reason.toString() }, 'gnani-stt: upstream socket closed');
            socket.gnaniSocket = null;
            socket.close();
          });

      } else if (obj.type === 'stop') {
        logger.info('gnani-stt: received stop from Cognigy');
        terminateSocket(socket);
      }

    } catch (err) {
      logger.error({ err }, 'gnani-stt: error handling message');
    }
  });

  socket.on('error', (err) => {
    logger.error({ err }, 'gnani-stt: Cognigy socket error');
  });

  socket.on('close', () => {
    logger.info('gnani-stt: Cognigy socket closed — cleaning up');
    terminateSocket(socket);
  });
};

module.exports = transcribe;