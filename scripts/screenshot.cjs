#!/usr/bin/env node
// Takes a screenshot of the hotelier dashboard with mock data.
// Usage: node scripts/screenshot.cjs [output_path]

const WebSocket = require('ws');
const { chromium } = require('playwright');

const outputPath = process.argv[2] || 'docs/screenshot-candidate.png';
const serverUrl = 'http://localhost:8080';

const guests = [
  { id: 'guest-alpha', name: 'Dev Guest Alpha', tags: ['business-default', 'frontend'] },
  { id: 'guest-beta', name: 'Dev Guest Beta', tags: ['business-default', 'backend'] },
];

const connections = [];

// Register guests via WebSocket
for (const g of guests) {
  const ws = new WebSocket('ws://localhost:8080/ws');
  ws.on('open', () => {
    ws.send(JSON.stringify({
      jsonrpc: '2.0', method: 'guest.register',
      params: { id: g.id, name: g.name, tags: g.tags }, id: 1
    }));
    console.log('Registered guest:', g.id);
  });
  ws.on('message', () => {});
  ws.on('error', (err) => console.error('WS error:', err.message));
  connections.push(ws);
}

(async () => {
  // Create a sample task so the cancel button is visible
  const taskRes = await fetch(`${serverUrl}/api/tasks`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: 'screenshot-demo-task',
      prompt: 'Build a responsive landing page with hero section and feature grid',
      repositories: 'https://github.com/example/webapp.git',
    }),
  });
  const task = await taskRes.json();
  console.log('Created task:', task.id);

  // Wait for UI to settle
  await new Promise(r => setTimeout(r, 1500));

  // Take screenshot
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  await page.goto(serverUrl);
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(1000);
  await page.screenshot({ path: outputPath });
  await browser.close();

  connections.forEach(c => c.close());
  console.log('Screenshot saved to', outputPath);
  process.exit(0);
})();
