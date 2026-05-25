const path = require('node:path');
const transcribe = async(logger, socket, url) => {
  const p = path.basename(url.split('?')[0]);
  switch (p) {
    case 'google':
      return require('./google')(logger, socket);
    case 'assemblyAI':
      return require('./assemblyAi')(logger, socket);
    case 'vosk':
      return require('./vosk')(logger, socket);
    case 'gladia':
      return require('./gladia')(logger, socket);
    case 'gnani':
      return require('./gnani')(logger, socket);
    default:
      logger.info(`unknown stt vendor: ${p}`);
      socket.close();
  }
};

module.exports = transcribe;