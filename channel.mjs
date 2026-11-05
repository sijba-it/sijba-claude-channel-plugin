#!/usr/bin/env node
// MCP channel server, spawned by Claude Code (stdio). Pure bridge: all sijba
// logic lives in main.py, reached over a unix socket (JSON lines).
//   main.py -> here : {type:"inbound", chat_id, text}            => channel event
//                     {type:"permission_verdict", request_id, behavior}
//   here -> main.py : {type:"reply", chat_id, text}              <= reply tool
//                     {type:"permission_request", ...}
import net from 'node:net'
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { z } from 'zod'

const SOCK = process.env.SIJBA_CHANNEL_SOCK
const RELAY = process.env.SIJBA_PERMISSION_RELAY === '1'
if (!SOCK) { console.error('SIJBA_CHANNEL_SOCK required'); process.exit(1) }

const capabilities = { experimental: { 'claude/channel': {} }, tools: {} }
if (RELAY) capabilities.experimental['claude/channel/permission'] = {}

const mcp = new Server({ name: 'sijba', version: '0.1.0' }, {
  capabilities,
  instructions:
    'Messages from the user arrive as <channel source="sijba" chat_id="...">. ' +
    'The user reads ONLY what you send with the reply tool (pass the chat_id from the tag); ' +
    'the terminal is not visible to them. Always send your final answer with reply, ' +
    'and keep it short. Use reply for a brief progress note only on long tasks. ' +
    'If the reply tool is deferred, load it first with ToolSearch (select:mcp__sijba__reply).',
})

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [{
    name: 'reply',
    description: 'Send a message back to the user in sijba',
    inputSchema: {
      type: 'object',
      properties: {
        chat_id: { type: 'string', description: 'chat_id from the <channel> tag' },
        text: { type: 'string', description: 'The message to send' },
      },
      required: ['chat_id', 'text'],
    },
  }],
}))

let sock = null
const out = o => sock && !sock.destroyed && sock.write(JSON.stringify(o) + '\n')

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  if (req.params.name !== 'reply') throw new Error(`unknown tool: ${req.params.name}`)
  const { chat_id, text } = req.params.arguments
  if (!sock || sock.destroyed) return { content: [{ type: 'text', text: 'sijba bridge offline, not sent' }], isError: true }
  out({ type: 'reply', chat_id: String(chat_id), text: String(text) })
  return { content: [{ type: 'text', text: 'sent' }] }
})

if (RELAY) {
  const PermissionRequest = z.object({
    method: z.literal('notifications/claude/channel/permission_request'),
    params: z.object({
      request_id: z.string(),
      tool_name: z.string(),
      description: z.string().optional(),
      input_preview: z.string().optional(),
    }),
  })
  mcp.setNotificationHandler(PermissionRequest, async ({ params }) => {
    out({ type: 'permission_request', ...params })
  })
}

await mcp.connect(new StdioServerTransport())

function connect() {
  sock = net.createConnection(SOCK, () => out({ type: 'hello' }))
  let buf = ''
  sock.on('data', async d => {
    buf += d
    let i
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i); buf = buf.slice(i + 1)
      if (!line.trim()) continue
      let m
      try { m = JSON.parse(line) } catch { continue }
      if (m.type === 'inbound') {
        await mcp.notification({
          method: 'notifications/claude/channel',
          params: { content: m.text, meta: { chat_id: String(m.chat_id) } },
        })
      } else if (m.type === 'permission_verdict') {
        await mcp.notification({
          method: 'notifications/claude/channel/permission',
          params: { request_id: m.request_id, behavior: m.behavior },
        })
      }
    }
  })
  sock.on('error', () => {})
  sock.on('close', () => setTimeout(connect, 2000))
}
connect()
