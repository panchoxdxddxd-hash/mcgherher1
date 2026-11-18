const { WebSocketServer } = require('ws');
const port = process.env.PORT || 8080;
const wss = new WebSocketServer({ port });
console.log('Relay activo en puerto ' + port);
wss.on('connection', ws => {
  ws.on('message', msg => {
    wss.clients.forEach(c => { if (c !== ws && c.readyState === 1) c.send(msg); });
  });
});
