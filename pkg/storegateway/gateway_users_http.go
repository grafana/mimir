// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/grafana/mimir/pkg/util"
)

const usersPageTemplate = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Store-gateway: bucket users</title>
	</head>
	<body>
		<h1>Store-gateway: bucket users</h1>
		<p>Current time: {{ .Now }}</p>
		<table border="1" cellpadding="5" style="border-collapse: collapse">
			<thead>
				<tr>
					<th>User</th>
				</tr>
			</thead>
			<tbody>
				{{ range .Users }}
				<tr>
					<td><a href="user/{{ . }}/blocks">{{ . }}</a></td>
				</tr>
				{{ end }}
			</tbody>
		</table>
	</body>
</html>`

var usersTemplate = template.Must(template.New("webpage").Parse(usersPageTemplate))

func (s *StoreGateway) UsersHandler(w http.ResponseWriter, req *http.Request) {
	userIDs, err := s.stores.scanUsers(req.Context())
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Can't read users: %s", err))
		return
	}

	util.RenderHTTPResponse(w, struct {
		Now   time.Time `json:"now"`
		Users []string  `json:"users,omitempty"`
	}{
		Now:   time.Now(),
		Users: userIDs,
	}, usersTemplate, req)
}
