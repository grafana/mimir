package templates

import (
	"fmt"
	tmplhtml "html/template"
	"net/url"
	"sort"
	tmpltext "text/template"
	"text/template/parse"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/alertmanager/template"
)

// topTemplates returns the name of all templates in tmpl that are not
// executed from a {{ template "name" }} block.
//
// All templates in text/template have a name and some text. In most cases,
// the name is set when the template is created using template.New(name) and
// the text is parsed when calling tmpl.Parse(text). This text can include
// actions for the template called "name", but also define other named templates
// using either {{ block "name" pipeline }}{{ end }} or {{ define "name" }}{{ end }}.
// topTemplates returns the names of all such templates, including the template
// "name", that are not executed from a {{ template "name" }} block and
// "name" if name contains text other than just template definitions.
func topTemplates(tmpl *tmpltext.Template) ([]string, error) {
	// definedTmpls is the list of all named templates in tmpl, including tmpl.
	definedTmpls := tmpl.Templates()

	// If the text contains just template definitions then ignore the template
	// "name" but keep all of its named templates
	candidateTmpls := make([]*tmpltext.Template, 0, len(definedTmpls))
	for _, next := range definedTmpls {
		if !(next.Name() == tmpl.ParseName && parse.IsEmptyTree(next.Root)) {
			candidateTmpls = append(candidateTmpls, next)
		}
	}

	executedTmpls := make(map[string]struct{}, len(candidateTmpls))
	for _, next := range candidateTmpls {
		err := checkTmpl(next, executedTmpls)
		if err != nil {
			return nil, fmt.Errorf("failed to check for occurrences of 'template': %w", err)
		}
	}

	results := make([]string, 0, len(candidateTmpls))
	for _, next := range candidateTmpls {
		name := next.Name()
		if _, ok := executedTmpls[name]; !ok {
			results = append(results, name)
		}
	}

	sort.Strings(results)

	return results, nil
}

// checkTmpl looks for all occurrences of {{ template "name" }} in the template.
// It adds the name of each template executed in executedTmpls.
func checkTmpl(tmpl *tmpltext.Template, executedTmpls map[string]struct{}) error {
	if tr := tmpl.Tree; tr == nil {
		return fmt.Errorf("template %s has nil parse tree", tmpl.Name())
	} else { // nolint
		checkListNode(tr.Root, executedTmpls)
		return nil
	}
}

// checkBranchNode checks the if and else branch for occurrences of
// {{ template "name" }} in if, for and with statements.
func checkBranchNode(node *parse.BranchNode, executedTmpls map[string]struct{}) {
	if node.List != nil {
		checkListNode(node.List, executedTmpls)
	}
	if node.ElseList != nil {
		checkListNode(node.ElseList, executedTmpls)
	}
}

// checkListNode checks each node in the list.
func checkListNode(node *parse.ListNode, executedTmpls map[string]struct{}) {
	for _, n := range node.Nodes {
		checkNode(n, executedTmpls)
	}
}

func checkNode(node parse.Node, executedTmpls map[string]struct{}) {
	switch node.Type() {
	// The node is an if statement (with optional else)
	case parse.NodeIf:
		n := node.(*parse.IfNode)
		checkBranchNode(&n.BranchNode, executedTmpls)
	// The node is a list of nodes. This occurs at the root of the template,
	// in the if and else branches of if statements, ranges and with statements
	case parse.NodeList:
		n := node.(*parse.ListNode)
		checkListNode(n, executedTmpls)
	// The node is a range statement
	case parse.NodeRange:
		n := node.(*parse.RangeNode)
		checkBranchNode(&n.BranchNode, executedTmpls)
	// The node is a template call, so add the name to the set of executed templates
	case parse.NodeTemplate:
		n := node.(*parse.TemplateNode)
		executedTmpls[n.Name] = struct{}{}
	// The node is a with statement
	case parse.NodeWith:
		n := node.(*parse.WithNode)
		checkBranchNode(&n.BranchNode, executedTmpls)
	}
}

// ParseTestTemplate parses the test template and returns the top-level definitions that should be interpolated as results.
func ParseTestTemplate(name string, text string) ([]string, error) {
	var tmpl *tmpltext.Template
	var capture template.Option = func(text *tmpltext.Template, _ *tmplhtml.Template) {
		tmpl = text
	}

	_, err := template.New(append(defaultOptionsPerKind(GrafanaKind), capture)...)
	if err != nil {
		return nil, err
	}

	tmpl, err = tmpl.New(name).Parse(text)
	if err != nil {
		return nil, err
	}

	topLevel, err := topTemplates(tmpl)
	if err != nil {
		return nil, err
	}

	return topLevel, nil
}

// TemplateFromTemplateDefinitions returns a *Template based on defaults and the provided template contents.
func TemplateFromTemplateDefinitions(templates []TemplateDefinition, logger log.Logger, externalURL string, options ...template.Option) (*Template, error) {
	seen := make(map[string]struct{})
	tmpls := make([]string, 0, len(templates))
	for _, tc := range templates {
		if _, ok := seen[tc.Name]; ok {
			level.Warn(logger).Log("msg", "template with same name is defined multiple times, skipping...", "template_name", tc.Name)
			continue
		}
		tmpls = append(tmpls, tc.Template)
		seen[tc.Name] = struct{}{}
	}

	tmpl, err := fromContent(append(defaultTemplatesPerKind(GrafanaKind), tmpls...), append(defaultOptionsPerKind(GrafanaKind), options...)...)
	if err != nil {
		return nil, err
	}
	extURL, err := url.Parse(externalURL)
	if err != nil {
		return nil, err
	}
	tmpl.ExternalURL = extURL
	return tmpl, nil
}
