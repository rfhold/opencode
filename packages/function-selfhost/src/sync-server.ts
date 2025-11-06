import { randomUUID } from "node:crypto"
import { Operator } from "opendal"
import type { ServerWebSocket } from "bun"

interface SessionData {
  secret?: string
  sessionID?: string
}

export class SyncServer {
  private operator: Operator
  private subscribers = new Map<string, Set<ServerWebSocket<unknown>>>()

  constructor(operator: Operator) {
    this.operator = operator
  }

  async getSessionData(sessionId: string): Promise<SessionData | null> {
    try {
      const data = await this.operator.read(`shares/${sessionId}/secret.json`)
      return JSON.parse(data.toString())
    } catch {
      return null
    }
  }

  async publish(sessionId: string, key: string, content: any) {
    const sessionData = await this.getSessionData(sessionId)
    if (!sessionData?.sessionID) {
      throw new Error("Session not found")
    }

    if (
      !key.startsWith(`session/info/${sessionData.sessionID}`) &&
      !key.startsWith(`session/message/${sessionData.sessionID}/`) &&
      !key.startsWith(`session/part/${sessionData.sessionID}/`)
    ) {
      throw new Error("Invalid key")
    }

    const storageKey = `share/${key}.json`
    await this.operator.write(storageKey, JSON.stringify(content))
  }

  async share(sessionID: string) {
    const shortName = SyncServer.shortName(sessionID)
    const existingSession = await this.getSessionData(shortName)

    if (existingSession?.secret) return existingSession.secret

    const secret = randomUUID()
    const sessionData = { secret, sessionID }
    await this.operator.write(`shares/${shortName}/secret.json`, JSON.stringify(sessionData))

    return secret
  }

  async getData(sessionId: string): Promise<Array<{key: string, content: any}>> {
    const sessionData = await this.getSessionData(sessionId)
    if (!sessionData?.sessionID) return []

    const entries = await this.operator.list("share/", { recursive: true })
    const result: Array<{key: string, content: any}> = []
    for (const entry of entries) {
      const path = entry.path()
      if (path.startsWith("share/session/") && path.includes(sessionData.sessionID!) && path.endsWith(".json")) {
        const content = await this.operator.read(path)
        const key = path.replace("share/", "").replace(".json", "")
        result.push({ key, content: JSON.parse(content.toString()) })
      }
    }
    return result
  }

  async assertSecret(sessionId: string, secret: string) {
    const sessionData = await this.getSessionData(sessionId)
    if (!sessionData || sessionData.secret !== secret) {
      throw new Error("Invalid secret")
    }
  }

  async clear(sessionId: string) {
    const sessionData = await this.getSessionData(sessionId)
    if (!sessionData?.sessionID) return

    try {
      const prefix = `session/message/${sessionData.sessionID}/`
      const entries = await this.operator.list(prefix)
      for (const entry of entries) {
        await this.operator.delete(entry.path())
      }

      await this.operator.delete(`session/info/${sessionData.sessionID}`)
    } catch (error) {
      console.error("Error clearing session data:", error)
    }
  }

  async loadSession(sessionId: string): Promise<SessionData | null> {
    try {
      const data = await this.operator.read(`shares/${sessionId}/secret.json`)
      return JSON.parse(data.toString())
    } catch (error) {
      console.error("Failed to load session:", error)
      return null
    }
  }

  static shortName(id: string) {
    return id.substring(id.length - 8)
  }

  subscribeWebSocket(topic: string, ws: ServerWebSocket<unknown>) {
    if (!this.subscribers.has(topic)) {
      this.subscribers.set(topic, new Set())
    }
    this.subscribers.get(topic)!.add(ws)
  }

  unsubscribeWebSocket(topic: string, ws: ServerWebSocket<unknown>) {
    const subscribers = this.subscribers.get(topic)
    if (subscribers) {
      subscribers.delete(ws)
      if (subscribers.size === 0) {
        this.subscribers.delete(topic)
      }
    }
  }

  publishToWebSockets(topic: string, message: string) {
    const subscribers = this.subscribers.get(topic)
    if (subscribers) {
      subscribers.forEach(ws => {
        if (ws.readyState === 1) { // OPEN state
          ws.send(message)
        }
      })
    }
  }
}