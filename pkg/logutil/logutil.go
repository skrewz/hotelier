// Package logutil formats session content for the process log.
//
// Everything the hotelier server and its guests write to stdout ends up in
// the systemd journal (or podman logs), which is read by whoever is
// troubleshooting and by whoever has root on the host. Agentic session
// content — prompts, model output, tool arguments and results — does not
// belong there: it is large, and it is a leakage risk. The journal should
// say that a task did something and how big it was; the task trace in the
// log store remains the place that keeps the content itself for the UI.
// See issue #241.
package logutil

import "fmt"

// Content describes a piece of session content by size only.
//
// Use it wherever a log line would otherwise interpolate the text:
//
//	log.Printf("[TASK] prompt: %s", logutil.Content(task.Prompt))
func Content(text string) string {
	return fmt.Sprintf("%d bytes", len(text))
}
