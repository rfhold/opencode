import { app, websocket } from "./api"

const port = Number(process.env.PORT) || 3000
const host = process.env.HOST || "127.0.0.1"

console.log(`OpenCode Self-Hosted Function Server running on ${host}:${port}`)
console.log(`Data directory: ./data`)
console.log(`WebSocket endpoint: ws://${host}:${port}/share_poll?id=<sessionId>`)

export default {
  port,
  hostname: host,
  fetch: app.fetch,
  websocket,
}