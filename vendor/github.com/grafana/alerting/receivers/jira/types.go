// Carbon copy of github.com/prometheus/alertmanager/notify/jira/types.go

package jira

import (
	"encoding/json"
)

type issue struct {
	Key        string       `json:"key,omitempty"`
	Fields     *issueFields `json:"fields,omitempty"`
	Transition *idNameValue `json:"transition,omitempty"`
}

type issueFields struct {
	Description any          `json:"description"`
	Issuetype   *idNameValue `json:"issuetype,omitempty"`
	Labels      []string     `json:"labels,omitempty"`
	Priority    *idNameValue `json:"priority,omitempty"`
	Project     *keyValue    `json:"project,omitempty"`
	Resolution  *idNameValue `json:"resolution,omitempty"`
	Summary     string       `json:"summary"`
	Status      *issueStatus `json:"status,omitempty"`

	Fields map[string]any `json:"-"`
}

type idNameValue struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type keyValue struct {
	Key string `json:"key"`
}

type issueStatus struct {
	Name           string   `json:"name"`
	StatusCategory keyValue `json:"statusCategory"`
}

type issueSearch struct {
	Expand     []string `json:"expand"`
	Fields     []string `json:"fields"`
	JQL        string   `json:"jql"`
	MaxResults int      `json:"maxResults"`
	StartAt    int      `json:"startAt"`
}

type issueSearchResult struct {
	Total  int     `json:"total"`
	Issues []issue `json:"issues"`
}

type issueTransitions struct {
	Transitions []idNameValue `json:"transitions"`
}

// MarshalJSON merges the struct issueFields and issueFields.CustomField together.
func (i issueFields) MarshalJSON() ([]byte, error) {
	jsonFields := map[string]any{
		"description": i.Description,
		"summary":     i.Summary,
	}

	if i.Issuetype != nil {
		jsonFields["issuetype"] = i.Issuetype
	}

	if i.Labels != nil {
		jsonFields["labels"] = i.Labels
	}

	if i.Priority != nil {
		jsonFields["priority"] = i.Priority
	}

	if i.Project != nil {
		jsonFields["project"] = i.Project
	}

	if i.Resolution != nil {
		jsonFields["resolution"] = i.Resolution
	}

	if i.Status != nil {
		jsonFields["status"] = i.Status
	}

	for key, field := range i.Fields {
		jsonFields[key] = field
	}

	return json.Marshal(jsonFields)
}

// adfDocument represents Atlassian Document Format structure (https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/)
type adfDocument struct {
	Version int       `json:"version"`
	Type    string    `json:"type"`
	Content []adfNode `json:"content"`
}

type adfNode struct {
	Type    string    `json:"type"`
	Content []adfNode `json:"content,omitempty"`
	Text    string    `json:"text,omitempty"`
}

func simpleAdfDocument(description string) adfDocument {
	return adfDocument{
		Version: 1,
		Type:    "doc",
		Content: []adfNode{
			{
				Type: "paragraph",
				Content: []adfNode{
					{
						Type: "text",
						Text: description,
					},
				},
			}},
	}
}
