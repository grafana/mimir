// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"os"
)

//go:embed report.html
var reportHTML []byte

// writeHTML writes the report as a page of its own: its data, and the charts and tables the page draws from it.
func writeHTML(path string, report Report) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	// The data sits in a script element, which a "</" in a request's text would end.
	data = bytes.ReplaceAll(data, []byte("</"), []byte(`<\/`))
	page := bytes.Replace(reportHTML, []byte("__REPORT_DATA__"), data, 1)
	return os.WriteFile(path, page, 0o644)
}
