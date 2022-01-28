// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/grafana/mimir/pkg/util"
)

const tenantsPageTemplate = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Store-gateway: bucket tenants</title>
	</head>
	<body>
		<h1>Store-gateway: bucket tenants</h1>
		<p>Current time: {{ .Now }}</p>
		<table border="1" cellpadding="5" style="border-collapse: collapse">
			<thead>
				<tr>
					<th>Tenant</th>
				</tr>
			</thead>
			<tbody>
				{{ range .Tenants }}
				<tr>
					<td><a href="tenant/{{ . }}/blocks">{{ . }}</a></td>
				</tr>
				{{ end }}
			</tbody>
		</table>
	</body>
</html>`

var tenantsTemplate = template.Must(template.New("webpage").Parse(tenantsPageTemplate))

func (s *StoreGateway) TenantsHandler(w http.ResponseWriter, req *http.Request) {
	tenantIDs, err := s.stores.scanUsers(req.Context())
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Can't read tenants: %s", err))
		return
	}

	util.RenderHTTPResponse(w, struct {
		Now     time.Time `json:"now"`
		Tenants []string  `json:"tenants,omitempty"`
	}{
		Now:     time.Now(),
		Tenants: tenantIDs,
	}, tenantsTemplate, req)
}
