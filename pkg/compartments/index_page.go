// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	_ "embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
)

//go:embed index_page.gohtml
var indexPageContent string

var indexPageTemplate = template.Must(template.New("compartments-index").Parse(indexPageContent))

type indexPageLink struct {
	Title string
	Path  string
}

type indexPageData struct {
	Title string
	Links []indexPageLink
}

// NewIndexPageHandler returns an http.Handler that renders an index page with one link per
// compartment. linkPathFormat is a link path containing the CompartmentIDPlaceholder, which is
// replaced with each compartment ID to build the per-compartment links.
func NewIndexPageHandler(pageTitle, linkPathFormat string, numCompartments int) http.Handler {
	data := indexPageData{Title: pageTitle, Links: make([]indexPageLink, numCompartments)}
	for id := range data.Links {
		data.Links[id] = indexPageLink{
			Title: fmt.Sprintf("Compartment %d", id),
			Path:  strings.ReplaceAll(linkPathFormat, CompartmentIDPlaceholder, strconv.Itoa(id)),
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := indexPageTemplate.Execute(w, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
