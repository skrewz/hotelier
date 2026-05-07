# General gotcha's for agents

- You will likely be working with a git worktree. Please orient yourself.
- Avoid accessing /tmp/ for temporary files. Use a temporary folder within the directory (and clean up) instead.
- Be podman-centric. Docker is not used here.
- Check if there are Makefile targets for building and/or linting; use them before handing over the task.
- When handling errors, make changes only if they relate to the specific error
- Proactively make use of web search tools for documentation, examples, versions etc
- If producing git commits, use gitmoji and conventional commits
- Under *no circumstances* are you allowed to push git commits.
- Do not use complete paths in the read tool if a relative path would do.
- Always load any web search skills available to you. They will almost always be relevant to your work.
- It does not make sense to ask the user "shall I continue" if you're still in Plan mode.

# Browser-side validation

Changes that touch JavaScript, HTML, CSS, or WebSocket communication **must** be validated in a headless browser. Go tests cannot catch client-side regressions.

**Why:** JS errors silently swallowed by empty `catch {}` blocks, duplicate `const` declarations, wrong DOM selectors, and WebSocket message format mismatches all pass Go tests but break the UI completely.

**How:**
1. Start the server: `./bin/hotelier &`
2. Launch a headless browser via Playwright to exercise the full path:
   ```js
   const { chromium } = require('playwright');
   const browser = await chromium.launch({ headless: true });
   const page = await browser.newPage();
   page.on('pageerror', err => console.error('JS ERROR:', err.message));
   await page.goto('http://localhost:8080/');
   await page.waitForLoadState('networkidle');
   // Check critical functions exist
   const hasConnectWS = await page.evaluate(() => typeof connectWS === 'function');
   console.log('connectWS:', hasConnectWS);
   // Check WebSocket connects
   page.on('websocket', ws => console.log('WS:', ws.url()));
   // Verify UI state
   const tasks = await page.$$('.task-item');
   console.log('Tasks visible:', tasks.length);
   await browser.close();
   ```
3. Verify: no JS errors, no console errors, expected functions exist, WebSocket connects, UI renders.
4. Kill the server: `pkill -f "bin/hotelier"`

**Common pitfalls to check:**
- No duplicate `const`/`let` declarations in the same scope
- No empty `catch {}` blocks — always log the error
- WebSocket params are objects, not strings (check `typeof msg.params`)
- DOM selectors match actual element IDs/classes
- `innerHTML` vs `textContent` — use `textContent` for user data, `innerHTML` only for trusted HTML

# Go formatting

- Run `make format` (gofumpt) before committing changes. gofumpt is stricter than `go fmt`.
- Run `make check-format` to verify formatting is correct.
- If gofumpt is not installed, install it with: `go install mvdan.cc/gofumpt@latest`

# Code coverage

- Run `make test-coverage` to generate a coverage report.
- When adding or modifying tests, ensure you are maintaining or improving coverage.
- Coverage output is written to `coverage.html` (HTML) and `coverage.out` (text).
- The `make test-coverage` target prints the total coverage percentage to stdout.
- Aim to keep test coverage above 80% for all new code paths.

# Agent working directory

- When the agent starts, it creates a temporary directory for working files.
- Use `os.MkdirTemp("", "hotelier-agent-*")` for per-session temp directories.
- Clean up temp directories with `os.RemoveAll()` when done.
