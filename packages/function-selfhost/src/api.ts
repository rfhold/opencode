import { Hono } from "hono"
import { createBunWebSocket } from "hono/bun"
import { Operator } from "opendal"
import { SyncServer } from "./sync-server"
import type { ServerWebSocket } from "bun"

const operator = new Operator("fs", { root: "./data" })
const syncServer = new SyncServer(operator)

const { upgradeWebSocket, websocket } = createBunWebSocket()

const app = new Hono()

app.get("/", (c) => c.text("OpenCode Self-Hosted Function Server"))

app.post("/share_create", async (c) => {
  try {
    const body = await c.req.json<{ sessionID: string }>()
    const sessionID = body.sessionID
    const short = SyncServer.shortName(sessionID)
    const secret = await syncServer.share(sessionID)

    const response = {
      secret,
      url: `${process.env.WEB_URL || "http://localhost:4321"}/s/${short}`,
    }
    return c.json(response)
  } catch (error) {
    console.error(`Error in /share_create:`, error)
    return c.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 500 })
  }
})

app.post("/share_delete", async (c) => {
  try {
    const body = await c.req.json<{ sessionID: string; secret: string }>()
    const sessionID = body.sessionID
    const secret = body.secret
    const short = SyncServer.shortName(sessionID)

    await syncServer.assertSecret(short, secret)
    await syncServer.clear(short)
    return c.json({})
  } catch (error) {
    console.error(`Error in /share_delete:`, error)
    return c.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 500 })
  }
})

app.post("/share_sync", async (c) => {
  try {
    const body = await c.req.json<{
      sessionID: string
      secret: string
      key: string
      content: any
    }>()
    const short = SyncServer.shortName(body.sessionID)

    await syncServer.assertSecret(short, body.secret)
    await syncServer.publish(short, body.key, body.content)

    const topic = `session:${short}`
    const message = JSON.stringify({ key: body.key, content: body.content })

    syncServer.publishToWebSockets(topic, message)

    return c.json({})
  } catch (error) {
    console.error(`Error in /share_sync:`, error)
    return c.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 500 })
  }
})

app.get("/share_data", async (c) => {
  try {
    const id = c.req.query("id")
    if (!id) return c.text("Error: Share ID is required", { status: 400 })

    await syncServer.loadSession(id)
    const data = await syncServer.getData(id)

    let info
    const messages: Record<string, any> = {}
    data.forEach((d) => {
      const [root, type, ..._splits] = d.key.split("/")
      if (root !== "session") return
      if (type === "info") {
        info = d.content
        return
      }
      if (type === "message") {
        messages[d.content.id] = {
          parts: [],
          ...d.content,
        }
      }
      if (type === "part") {
        messages[d.content.messageID].parts.push(d.content)
      }
    })

    return c.json({ info, messages })
  } catch (error) {
    console.error(`Error in /share_data:`, error)
    return c.json({ error: error instanceof Error ? error.message : "Unknown error" }, { status: 500 })
  }
})

app.get(
  "/share_poll",
  upgradeWebSocket((c) => {
    const sessionId = c.req.query("id")

    if (!sessionId) {
      return {
        onOpen(_event, ws) {
          ws.close(1008, "Session ID required")
        }
      }
    }

    return {
      onOpen: async (_event, ws) => {

        const topic = `session:${sessionId}`
        const rawWs = ws.raw as ServerWebSocket<unknown>
        rawWs.subscribe(topic)
        syncServer.subscribeWebSocket(topic, rawWs)

        const sessionData = await syncServer.getSessionData(sessionId)
        if (sessionData) {
          const data = await syncServer.getData(sessionId)
          data.forEach(({ key, content }) => ws.send(JSON.stringify({ key, content })))
        }
      },

      onMessage(_event, _ws) {
      },

      onClose(_event, ws) {
        const topic = `session:${sessionId}`
        const rawWs = ws.raw as ServerWebSocket<unknown>
        rawWs.unsubscribe(topic)
        syncServer.unsubscribeWebSocket(topic, rawWs)
      },

      onError(event, _ws) {
        console.error("WebSocket error:", event)
      }
    }
  })
)

app.get("/s/:sessionId", async (c) => {
  const sessionId = c.req.param("sessionId");
  const data = await syncServer.getSessionData(sessionId);
  if (!data) return c.text("Not Found", 404);
  return c.json(data);
});

app.all("*", (c) => c.text("Not Found", { status: 404 }))

export { app, syncServer, websocket }
