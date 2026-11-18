const { WebSocketServer } = require('ws');
const port = process.env.PORT || 8080;
const wss = new WebSocketServer({ port });

const codes = new Map();

function genCode() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let res = '';
  for (let i = 0; i < 5; i++) res += chars[Math.floor(Math.random() * chars.length)];
  return codes.has(res) ? genCode() : res;
}

wss.on('connection', (ws) => {
  ws.binaryType = 'nodebuffer';
  let isHost = false;
  let roomCode = null;
  let peerMap = new Map();

  ws.on('message', (data) => {
    if (!Buffer.isBuffer(data) || data.length === 0) return;
    const pkt = data[0];

    // Ping / Handshake check (0x00)
    if (pkt === 0x00) {
      const version = data.length > 1 ? data[1] : 1;
      const brand = Buffer.from('RenderRelay');
      const resp = Buffer.alloc(3 + brand.length);
      resp[0] = 0x00;
      resp[1] = version;
      resp[2] = brand.length;
      brand.copy(resp, 3);
      ws.send(resp);
      return;
    }

    // Host Request (0x01)
    if (pkt === 0x01 && !isHost) {
      isHost = true;
      roomCode = genCode();
      codes.set(roomCode, { host: ws, peers: peerMap });
      const codeBuf = Buffer.from(roomCode, 'utf8');
      const resp = Buffer.alloc(2 + codeBuf.length);
      resp[0] = 0x02;
      resp[1] = codeBuf.length;
      codeBuf.copy(resp, 2);
      ws.send(resp);
      return;
    }

    // Client Join Request (0x03)
    if (pkt === 0x03 && !isHost && data.length > 2) {
      const len = data[1];
      const reqCode = data.slice(2, 2 + len).toString('utf8').toUpperCase();
      const room = codes.get(reqCode);
      if (!room) {
        ws.send(Buffer.from([0xFF, 0x01]));
        return;
      }
      const peerId = Math.random().toString(36).substring(2, 10);
      room.peers.set(peerId, ws);
      ws.room = room;
      ws.peerId = peerId;

      const idBuf = Buffer.from(peerId, 'utf8');
      const notifyHost = Buffer.alloc(2 + idBuf.length);
      notifyHost[0] = 0x04;
      notifyHost[1] = idBuf.length;
      idBuf.copy(notifyHost, 2);
      room.host.send(notifyHost);
      return;
    }

    // Relay ICE / Description packets (0x05 / 0x06)
    if ((pkt === 0x05 || pkt === 0x06) && data.length > 2) {
      const idLen = data[1];
      const targetId = data.slice(2, 2 + idLen).toString('utf8');
      const payload = data.slice(2 + idLen);

      if (isHost) {
        const client = peerMap.get(targetId);
        if (client && client.readyState === 1) {
          const out = Buffer.alloc(1 + payload.length);
          out[0] = pkt;
          payload.copy(out, 1);
          client.send(out);
        }
      } else if (ws.room && ws.room.host.readyState === 1) {
        const myIdBuf = Buffer.from(ws.peerId, 'utf8');
        const out = Buffer.alloc(2 + myIdBuf.length + payload.length);
        out[0] = pkt;
        out[1] = myIdBuf.length;
        myIdBuf.copy(out, 2);
        payload.copy(out, 2 + myIdBuf.length);
        ws.room.host.send(out);
      }
    }
  });

  ws.on('close', () => {
    if (isHost && roomCode) {
      codes.delete(roomCode);
      peerMap.forEach((c) => c.close());
    } else if (ws.room && ws.peerId) {
      ws.room.peers.delete(ws.peerId);
    }
  });
});

console.log('EaglerRelay activo en puerto ' + port);
