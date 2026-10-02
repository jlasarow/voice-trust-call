// Voice Trust Call -- minimal signaling server.
//
// This server never sees or touches call audio. Its only job is to let two
// phones that both know the same short call code find each other and swap
// the handful of technical messages (an SDP offer/answer and ICE candidates)
// needed to open a direct WebRTC connection between them. Once that
// connection is open, audio flows phone-to-phone and this server is no
// longer involved for that call.

const path = require("path");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const app = express();

app.use(express.static(path.join(__dirname, "public")));
app.get("/healthz", (req, res) => res.status(200).send("ok"));

const server = app.listen(PORT, () => {
  console.log("Voice Trust Call listening on " + PORT);
});

const wss = new WebSocketServer({ server, path: "/ws" });

/** @type {Map<string, Set<import('ws').WebSocket>>} */
const rooms = new Map();

function broadcast(room, msg, exceptWs) {
  const members = rooms.get(room);
  if (!members) return;
  const text = JSON.stringify(msg);
  for (const member of members) {
    if (member !== exceptWs && member.readyState === member.OPEN) {
      member.send(text);
    }
  }
}

function leaveRoom(ws) {
  const room = ws.roomCode;
  if (!room) return;
  const members = rooms.get(room);
  if (!members) return;
  members.delete(ws);
  if (members.size === 0) {
    rooms.delete(room);
  } else {
    broadcast(room, { type: "peer-left" }, ws);
  }
  ws.roomCode = null;
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed frames
    }

    if (msg.type === "join") {
      const room = String(msg.room || "").trim();
      if (!/^[0-9]{4,8}$/.test(room)) {
        ws.send(JSON.stringify({ type: "error", reason: "bad-room-code" }));
        return;
      }
      if (!rooms.has(room)) rooms.set(room, new Set());
      const members = rooms.get(room);
      if (members.size >= 2) {
        ws.send(JSON.stringify({ type: "room-full" }));
        return;
      }
      members.add(ws);
      ws.roomCode = room;
      ws.send(JSON.stringify({ type: "joined", peers: members.size - 1 }));
      broadcast(room, { type: "peer-joined" }, ws);
      return;
    }

    // every other message type (offer / answer / ice) is just relayed
    // to the other member of the same room, unread and unmodified.
    if (ws.roomCode) {
      broadcast(ws.roomCode, msg, ws);
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});

// drop dead connections so rooms don't fill up with ghosts
const interval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      leaveRoom(ws);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

wss.on("close", () => clearInterval(interval));
