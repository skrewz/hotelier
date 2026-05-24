#!/usr/bin/env python3
"""Combined script: interact with pi via RPC, capture wire traffic, then redact.

The redaction step replaces sensitive values with generic placeholders.
No sensitive values are hardcoded in this script.
"""

import subprocess
import json
import sys
import re
import os
from datetime import datetime, timezone, timedelta

# Configuration
CAPTURE_FILE = "/tmp/pi-rpc-capture.log"
REDACTED_FILE = "/tmp/pi-rpc-capture-redacted.log"
PROMPT = "What's my hostname?"

# Hardwired base timestamp: 2026-05-24 UTC midnight
TODAY_UTC_MIDNIGHT = datetime(2026, 5, 24, 0, 0, 0, 0, tzinfo=timezone.utc)


def ts():
    """Return an ISO timestamp."""
    return datetime.now().isoformat()


def log(tag, data, f):
    """Log a line to both stderr and the capture file."""
    line = f"[{tag}] {ts()}: {data}"
    print(line, file=sys.stderr)
    f.write(line + "\n")


def run_rpc_capture():
    """Run pi in RPC mode and capture all wire traffic."""
    with open(CAPTURE_FILE, "w") as f:
        f.write(f"pi RPC wire capture \u2014 {ts()}\n")
        f.write(f"Prompt: {PROMPT}\n")
        f.write("=" * 80 + "\n\n")
        
        log("INFO", f"Starting pi RPC, capture \u2192 {CAPTURE_FILE}", f)
        log("INFO", f"Prompt: {PROMPT}", f)

        # Start pi in RPC mode with no session persistence
        proc = subprocess.Popen(
            ["pi", "--mode", "rpc", "--no-session"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        log("INFO", f"pi process started (PID: {proc.pid})", f)
        log("INFO", "\u2500" * 80, f)

        # Send the prompt command
        cmd = {"type": "prompt", "message": PROMPT, "id": "req-1"}
        json_cmd = json.dumps(cmd)
        log("STDIN\u2192pi", json_cmd, f)
        proc.stdin.write(json_cmd + "\n")
        proc.stdin.flush()

        # Read events until agent_end
        event_count = 0
        try:
            for line in proc.stdout:
                line = line.rstrip("\n").rstrip("\r")
                if not line:
                    continue

                event_count += 1
                log("STDOUT\u2190pi", line, f)

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                etype = event.get("type", "?")

                if etype == "message_update":
                    ame = event.get("assistantMessageEvent", {})
                    delta_type = ame.get("type", "")
                    if delta_type == "text_delta":
                        text = ame.get("delta", "")
                        sys.stdout.write(text)
                        sys.stdout.flush()
                    elif delta_type == "done":
                        sys.stdout.write("\n")

                elif etype == "tool_execution_start":
                    tool = event.get("toolName", "?")
                    args = event.get("args", {})
                    sys.stdout.write(f"\n\U0001f527 Tool: {tool} ({args})\n")

                elif etype == "tool_execution_end":
                    tool = event.get("toolName", "?")
                    sys.stdout.write(f"\u2705 Tool {tool} finished\n")

                elif etype == "agent_end":
                    sys.stdout.write("\n\n" + "=" * 80 + "\n")
                    sys.stdout.write("Agent finished.\n")
                    break

        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            proc.wait(timeout=5)

        log("INFO", "\u2500" * 80, f)
        log("INFO", f"Total events received: {event_count}", f)
        log("INFO", f"Full capture saved to: {CAPTURE_FILE}", f)

        print(f"\n\U0001f4cb Full capture saved to: {CAPTURE_FILE}")


def compute_offset():
    """Compute the time offset from the first timestamp in the capture to today's UTC midnight."""
    with open(CAPTURE_FILE, "r") as f:
        first_line = f.readline()

    # Parse the first line for the timestamp
    match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", first_line)
    if match:
        first_ts = datetime.fromisoformat(match.group(0))
        # Assume UTC if no timezone info
        if first_ts.tzinfo is None:
            first_ts = first_ts.replace(tzinfo=timezone.utc)
        offset = first_ts - TODAY_UTC_MIDNIGHT
        return offset
    return timedelta(0)


def redact_capture(offset):
    """Redact sensitive information from the capture file."""
    def offset_iso(match):
        """Offset an ISO timestamp by the computed delta."""
        ts = match.group(0)
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        shifted = dt - offset
        return shifted.isoformat()

    def offset_epoch(match):
        """Offset an epoch millisecond timestamp."""
        val = int(match.group(0))
        # Convert offset to milliseconds
        offset_ms = int(offset.total_seconds() * 1000)
        return str(val - offset_ms)

    # Read the capture file
    with open(CAPTURE_FILE, "r") as f:
        content = f.read()

    # Apply redactions
    # Offset ISO timestamps
    content = re.sub(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+",
        offset_iso,
        content
    )

    # Offset epoch timestamps in JSON (milliseconds)
    content = re.sub(
        r'"timestamp":(\d{13})',
        lambda m: '"timestamp":' + str(int(m.group(1)) - int(offset.total_seconds() * 1000)),
        content
    )

    # Model name - use a generic placeholder
    content = re.sub(r"[A-Z][A-Z0-9]+-\d+\.\d+-[A-Za-z]+-[A-Z0-9_]+\.gguf", "actual-model-name", content)

    # Hostname - replace any hostname-like patterns with a generic one
    # Match the hostname in tool output and thinking content
    content = re.sub(r"devvm-[a-z]+", "devvm", content)

    # Provider - replace with generic placeholder
    # Use a pattern that matches the provider name without hardcoding it
    content = re.sub(r'"provider":"[a-z]+"', '"provider":"example-provider"', content)

    # Model ID - replace with generic placeholder
    # Use a pattern that matches the model ID without hardcoding it
    content = re.sub(r'"model":"[a-z-]+"', '"model":"example-model-id"', content)



    # Capture file path - replace with generic placeholder
    content = re.sub(r"/tmp/pi-rpc-capture\.log", "/tmp/example-capture.log", content)

    # PID - replace with generic placeholder
    content = re.sub(r'PID:\s*\d+', 'PID: 12345', content)

    # Write the redacted content
    with open(REDACTED_FILE, "w") as f:
        f.write(content)

    print(f"\n\U0001f4cb Redacted capture saved to: {REDACTED_FILE}")


def main():
    """Main function to run RPC capture and redact."""
    print("Running pi RPC capture...")
    run_rpc_capture()

    print("\nComputing timestamp offset...")
    offset = compute_offset()
    print(f"Offset: {offset}")

    print("Redacting capture file...")
    redact_capture(offset)


if __name__ == "__main__":
    main()
