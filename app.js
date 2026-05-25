const express = require('express');
const app = express();
const Websocket = require('ws');
const opts = Object.assign({level: process.env.LOGLEVEL || 'info'});
const logger = require('pino')(opts);
const port = process.env.HTTP_PORT || 3000;
const routes = require('./lib/tts');
app.locals = {...app.locals, logger};

const isValidApiKey = (hdr, apiKey) => {
  if (!apiKey) return true; // no API_KEY configured — allow all (dev mode)
  const arr = /^Bearer (.*)$/.exec(hdr);
  return (arr && arr[1] === apiKey);
};

const isValidQueryKey = (requestUrl) => {
  const url = new URL(requestUrl, 'http://localhost');
  const queryKey = url.searchParams.get('key');
  return queryKey === process.env.API_KEY;
};

const verifyApiKey = (req, res, next) => {
  if (!isValidApiKey(req.headers['authorization'], process.env.API_KEY)) {
    return res.status(403).json({ message: 'Forbidden' });
  }
  next();
};

/* set up a websocket server for the STT api */
const transcribe = require('./lib/stt/');
const transcribeWsServer = new Websocket.Server({ noServer: true });
transcribeWsServer.setMaxListeners(0);
transcribeWsServer.on('connection', transcribe.bind(null, logger));

/* set up a websocket server for the TTS Streaming api */
const ttsStreaming = require('./lib/tts/streaming');
const ttsWsServer = new Websocket.Server({ noServer: true });
ttsWsServer.setMaxListeners(0);
ttsWsServer.on('connection', ttsStreaming.bind(null, logger));


/* set up the http server for the TTS api */
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));
app.use('/synthesize', verifyApiKey, routes);
app.use((err, req, res, next) => {
  logger.error(err, 'burped error');
  res.status(err.status || 500).json({msg: err.message});
});

const server = app.listen(port, () => {
  logger.info(`Example Cognigy Voice Gateway speech server listening at http://localhost:${port}`);
});

/* handle websocket upgrade requests */
server.on('upgrade', (request, socket, head) => {
  logger.debug({
    url: request.url,
    headers: request.headers,
  }, 'received upgrade request');

  /* verify the path starts with /transcribe */
  if (!request.url.startsWith('/transcribe') && !request.url.startsWith('/synthesize')) {
    logger.info(`unhandled path: ${request.url}`);
    return socket.write('HTTP/1.1 404 Not Found \r\n\r\n', () => socket.destroy());
  }

  /* verify the api key —
     Cognigy VG sends:        Authorization: Bearer <API_KEY>   (header-based)
     Browser demo sends:      wss://...?key=<API_KEY>           (query param)
     Either is accepted; both missing = reject.
  */
  const validHeader = isValidApiKey(request.headers['authorization'], process.env.API_KEY);
  const validQuery  = isValidQueryKey(request.url);

  if (!validHeader && !validQuery) {
    logger.info(`rejected WS — invalid auth. url: ${request.url}`);
    return socket.write('HTTP/1.1 403 Forbidden \r\n\r\n', () => socket.destroy());
  }

  /* complete the upgrade */
  transcribeWsServer.handleUpgrade(request, socket, head, (ws) => {
    logger.info(`upgraded to websocket, url: ${request.url}`);
    if (request.url.startsWith('/synthesize')) {
      ttsWsServer.emit('connection', ws, request.url);
    } else if (request.url.startsWith('/transcribe')) {
      transcribeWsServer.emit('connection', ws, request.url);
    }
  });
});