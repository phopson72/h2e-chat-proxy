// here2elevate website assistant proxy (v2)
// - System prompt lives on the server (system-prompt.txt), never taken from the browser
// - Only here2elevate.com may call it (CORS allowlist + Origin check)
// - Per-visitor and daily rate limits, message size limits
// - Optional streaming (stream: true) so replies appear word by word
// - GET /health for Azure health checks and the keep-warm ping

const fs = require('fs');
const path = require('path');
const express = require('express');

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const ANTHROPIC_URL = process.env.ANTHROPIC_URL || 'https://api.anthropic.com/v1/messages';
const MODEL = process.env.CHAT_MODEL || 'claude-sonnet-4-6';
const MAX_TOKENS = parseInt(process.env.CHAT_MAX_TOKENS || '400', 10);

const ALLOWED_ORIGINS = new Set(
  ['https://here2elevate.com', 'https://www.here2elevate.com']
    .concat((process.env.EXTRA_ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean))
);

// Limits (override in Azure Environment variables if needed)
const PER_IP_LIMIT = parseInt(process.env.PER_IP_LIMIT || '20', 10);           // requests
const PER_IP_WINDOW_MS = parseInt(process.env.PER_IP_WINDOW_MIN || '10', 10) * 60 * 1000;
const DAILY_LIMIT = parseInt(process.env.DAILY_LIMIT || '500', 10);            // all visitors combined
const MAX_MESSAGES = 20;          // conversation turns kept (oldest dropped)
const MAX_MESSAGE_CHARS = 2000;   // per message
const MAX_TOTAL_CHARS = 12000;    // whole conversation

const SYSTEM_PROMPT = fs.readFileSync(path.join(__dirname, 'system-prompt.txt'), 'utf8');

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);
app.use(express.json({ limit: '64kb' }));

// ---- CORS: allow only the H2E website ----
app.use((req, res, next) => {
  const origin = req.get('Origin');
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.set('Access-Control-Allow-Origin', origin);
    res.set('Vary', 'Origin');
    res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.set('Access-Control-Allow-Headers', 'Content-Type');
    res.set('Access-Control-Max-Age', '86400');
  }
  if (req.method === 'OPTIONS') return res.sendStatus(origin && ALLOWED_ORIGINS.has(origin) ? 204 : 403);
  next();
});

// ---- Health check / keep-warm ----
app.get(['/', '/health'], (req, res) => res.json({ ok: true }));

// ---- Rate limiting (in memory; resets if the app restarts) ----
const hits = new Map();
let day = new Date().toISOString().slice(0, 10);
let dayCount = 0;

function clientIp(req) {
  const xf = (req.get('X-Forwarded-For') || '').split(',')[0].trim();
  return (xf || req.ip || 'unknown').replace(/:\d+$/, '');
}

function rateLimited(req) {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== day) { day = today; dayCount = 0; hits.clear(); }
  if (dayCount >= DAILY_LIMIT) return 'daily';
  const ip = clientIp(req);
  const now = Date.now();
  const list = (hits.get(ip) || []).filter(t => now - t < PER_IP_WINDOW_MS);
  if (list.length >= PER_IP_LIMIT) { hits.set(ip, list); return 'ip'; }
  list.push(now);
  hits.set(ip, list);
  dayCount++;
  return null;
}

// ---- Validate and trim the conversation sent by the browser ----
function cleanMessages(input) {
  if (!Array.isArray(input) || input.length === 0) return null;
  let msgs = input
    .filter(m => m && (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string')
    .map(m => ({ role: m.role, content: m.content.slice(0, MAX_MESSAGE_CHARS) }))
    .filter(m => m.content.trim().length > 0);
  msgs = msgs.slice(-MAX_MESSAGES);
  while (msgs.length && msgs[0].role !== 'user') msgs.shift();      // must start with the visitor
  let total = msgs.reduce((n, m) => n + m.content.length, 0);
  while (total > MAX_TOTAL_CHARS && msgs.length > 1) {              // drop oldest turns if too long
    total -= msgs.shift().content.length;
    while (msgs.length && msgs[0].role !== 'user') total -= msgs.shift().content.length;
  }
  if (!msgs.length || msgs[msgs.length - 1].role !== 'user') return null;
  return msgs;
}

app.post('/chat', async (req, res) => {
  const origin = req.get('Origin');
  if (!origin || !ALLOWED_ORIGINS.has(origin)) {
    return res.status(403).json({ error: 'Not allowed' });
  }
  const limited = rateLimited(req);
  if (limited) {
    return res.status(429).json({ error: limited === 'daily'
      ? 'The assistant is busy right now. Please email info@here2elevate.com or call (317) 499-7349.'
      : 'Too many messages in a short time. Please wait a few minutes and try again.' });
  }
  const messages = cleanMessages(req.body && req.body.messages);
  if (!messages) return res.status(400).json({ error: 'Invalid message' });
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: 'Assistant is not configured' });

  const stream = req.body.stream === true;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 60000);

  try {
    const upstream = await fetch(ANTHROPIC_URL, {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: MAX_TOKENS,
        system: [{ type: 'text', text: SYSTEM_PROMPT, cache_control: { type: 'ephemeral' } }],
        messages,
        stream
      })
    });

    if (!upstream.ok) {
      const detail = await upstream.text().catch(() => '');
      console.error('Anthropic error', upstream.status, detail.slice(0, 500));
      return res.status(502).json({ error: 'The assistant is unavailable right now. Please try again shortly.' });
    }

    if (!stream) {
      const data = await upstream.json();
      // Same shape as before so the current website widget keeps working
      return res.json({ content: data.content, stop_reason: data.stop_reason });
    }

    // Streaming: forward only the text as Server-Sent Events
    res.status(200);
    res.set({
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.flushHeaders();

    const decoder = new TextDecoder();
    let buffer = '';
    for await (const chunk of upstream.body) {
      buffer += decoder.decode(chunk, { stream: true });
      let idx;
      while ((idx = buffer.indexOf('\n\n')) !== -1) {
        const event = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const dataLine = event.split('\n').find(l => l.startsWith('data:'));
        if (!dataLine) continue;
        let evt;
        try { evt = JSON.parse(dataLine.slice(5).trim()); } catch { continue; }
        if (evt.type === 'content_block_delta' && evt.delta && evt.delta.type === 'text_delta') {
          res.write('data: ' + JSON.stringify({ t: evt.delta.text }) + '\n\n');
        } else if (evt.type === 'error') {
          console.error('Anthropic stream error', JSON.stringify(evt).slice(0, 500));
          res.write('data: ' + JSON.stringify({ error: 'The assistant hit a problem. Please try again.' }) + '\n\n');
        }
      }
    }
    res.write('data: [DONE]\n\n');
    res.end();
  } catch (err) {
    console.error('Proxy error', err && err.name, err && err.message);
    if (!res.headersSent) {
      res.status(504).json({ error: 'The assistant took too long to respond. Please try again.' });
    } else {
      res.write('data: ' + JSON.stringify({ error: 'The connection was interrupted. Please try again.' }) + '\n\n');
      res.end();
    }
  } finally {
    clearTimeout(timer);
  }
});

app.use((req, res) => res.status(404).json({ error: 'Not found' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('H2E proxy v2 running on port ' + PORT));
