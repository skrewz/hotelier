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

## Mandatory integration test: `make test-integration`

Every turn that touches client-side code **must** run the integration test suite and inspect the screenshots:

```bash
make test-integration
```

This starts a real server, registers a guest via WebSocket, simulates tool call log entries (including `[TOOL_START]`, `[TOOL_OUTPUT]`, `[TOOL_END]` with both success and error paths), and then uses Playwright to exercise the **real user flow**:

1. Navigate to the page and wait for the task list to populate (via real WebSocket messages from the Go test)
2. Click the task item — this triggers `selectTask()` → `refreshTaskDetail()` → `renderTaskDetail()`
3. Wait for any pending WebSocket updates to settle
4. Assert structural properties (tool block count, statuses, `<pre>` usage, etc.)
5. Take full-page `.png` screenshots at each stage

The screenshots are saved to a temp directory and their paths are logged in the test output. **You must read the screenshots** and verify visually that:

- The detail view renders under the "Task Detail" tab (not the "Tasks" tab)
- Tool blocks render with correct headers (tool name, ID, status label)
- Tool output is inside `<pre>` elements within the tool block body — not as separate plain-text log lines
- `[TOOL_OUTPUT]` and `[TOOL_END]` lines are consumed by the tool block, not duplicated as plain text below it
- Plain-text messages appear correctly between tool blocks
- Error tool blocks show the correct error status

> **Principle:** The Playwright script must follow the real user journey — navigate, interact, observe — not inject DOM elements and call rendering functions directly. Injected DOM bypasses the actual code paths (WebSocket delivery, task list rendering, `selectTask`, tab switching) that were the source of real bugs. If the test doesn't exercise the same functions a user would trigger, it isn't a true integration test.

If the screenshot shows any of the issues from the previous commit (tool output lines duplicated as plain text outside tool blocks), **stop and fix the rendering logic before proceeding**.

## Test ownership and test-suite deliberation

The integration test suite (`test/validate_tool_ui_test.go`) is **your responsibility** — not the user's. Treat it as living code that evolves alongside the UI.

At the end of every turn where you modify client-side code, deliberate on these questions **before handing back**:

1. **Does the diff suggest the integration test should be extended?**
   - Have you changed how tool blocks are rendered? → Add or update assertions about block structure, status labels, or output containment.
   - Have you changed the `parseToolLine` regexes? → Add test cases for edge-case formats (e.g. tool names with hyphens, empty args, multi-line output).
   - Have you added a new log level or message type? → Add corresponding test entries.
   - Have you changed CSS / layout? → The screenshot inspection catches regressions, but consider whether a new structural assertion would be more reliable.

2. **Should the test take additional screenshots?**
   - Are you introducing a new UI state (e.g. empty task, loading spinner, empty state, error banner)? → Add a `takeScreenshot` call at that point so future agents can visually inspect it.
   - Is the change likely to produce visual regressions that assertions can't catch (spacing, alignment, colour)? → Add a screenshot.

3. **Do the existing screenshots still make sense?**
   - Run `make test-integration` with `SCREENSHOT_DIR=/tmp/hotelier-screenshots` and read the saved `.png` files. If the rendered output no longer matches what the test was designed to validate, update the test data and assertions accordingly.

**Rule of thumb:** If the UI can look wrong in ways that structural assertions can't catch, a screenshot is the safety net. When in doubt, add one.

**How to extend the test:** The Playwright script is embedded inline in `validateUI()` inside `test/validate_tool_ui_test.go`. To add a screenshot, call `await takeScreenshot('04-descriptive-name')` at the appropriate point in the script. To add new log entries, extend `testLogEntries()`. To add assertions, extend the `checks` array in the inline script. To change the user interaction sequence (e.g. add a new step between clicking the task and validating), insert `await` calls before the `page.evaluate()` block.

## Manual Playwright smoke test

For quick checks outside the integration test suite:

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

### UI screenshots

The README includes a screenshot of the dashboard at `docs/screenshot.png`.
When making UI changes, **consider** whether the screenshot should be updated.
Do this by comparing the old and new screenshots side by side:

1. Generate a candidate screenshot:
   ```bash
   make docs/screenshot-candidate.png
   ```
   This ensures the server binary is built, starts the server, mock-registers
   two guests via WebSocket, takes a Playwright screenshot, and saves it to
   `docs/screenshot-candidate.png`.

2. **Read both screenshots** and compare them visually

3. Decide whether the change is significant enough to warrant an update:
   - **Update it** if the change is visually significant (new sections, layout
     shifts, new controls, major colour/typography changes)
   - **Skip it** for minor tweaks (bug fixes, small label changes, edge-case
     handling) that don't alter the overall look of the dashboard

4. If updating:
   ```bash
   mv -v docs/screenshot-candidate.png docs/screenshot.png
   ```
   Otherwise:
   ```bash
   rm docs/screenshot-candidate.png
   ```

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
