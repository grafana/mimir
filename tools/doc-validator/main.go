// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/gomarkdown/markdown/ast"
	"github.com/gomarkdown/markdown/parser"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
)

var (
	// This regular expression matches Hugo relref syntax. If a link is matching, the returned capture groups
	// contain the path and anchor (both optional).
	hugoRelref = regexp.MustCompile(`^\{\{<\s*relref\s+"([^"#]*)#?([^"]*)"\s*>\}\}$`)

	// This regular expression is used to remove characters when generating the anchor similarly to how Hugo does it.
	hugoAnchorCleanup = regexp.MustCompile(`\.+`)

	// This regular expression is used to replace some characters with hyphen when generating the anchor similarly
	// to how Hugo does it.
	hugoAnchorReplacement = regexp.MustCompile(`[^a-z0-9\-_/]+/?`)

	errLinkInvalid         = `The link %q is invalid. Ensure the link is a http or https URL, an anchor starting with # or a relative link in the Hugo format (eg. '{{< relref "path-to.md" >}}')`
	errLinkNotExist        = "The link %q references a document which does not exist"
	errAnchorNotExist      = "The anchor %q does not exist in the file %v. Available anchors are: %s"
	errImageLocatedOutside = `The image %q must be located in the same directory of the markdown file %q`
	errImageNotIndex       = `The image %q can only be linked from an index.md or _index.md file. Move %q to a Hugo bundle and the image inside the bundle directory.`
	errImageNotExist       = `The image %q does not exist`
)

func main() {
	// Parse the generator flags.
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Printf("Usage: doc-validator path-to-doc\n")
		os.Exit(1)
	}

	docRoot := flag.Arg(0)

	// We find all available anchors across the whole documentation, so that we can
	// validate if anchor links are valid.
	anchors, err := parseMarkdownAnchorsFromFiles(docRoot)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	errs := validateMarkdownFiles(docRoot, anchors)
	if len(errs) == 0 {
		return
	}

	for _, err := range errs {
		fmt.Println(err.Error() + "\n")
	}
	os.Exit(1)
}

func parseMarkdownAnchorsFromFiles(docRoot string) (markdownAnchors, error) {
	anchors := markdownAnchors{}

	err := filepath.Walk(docRoot, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}

		found, err := parseMarkdownAnchorsFromFile(path)
		if err != nil {
			return err
		}

		anchors.add(path, found...)
		return nil
	})

	return anchors, errors.Wrap(err, "unable to parse markdown anchors")
}

func parseMarkdownAnchorsFromFile(file string) ([]string, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read file %s", file)
	}

	return parseMarkdownAnchorsFromFileContent(data)
}

func parseMarkdownAnchorsFromFileContent(data []byte) ([]string, error) {
	// Remove frontmatter because not supported by the parsing library.
	data = removeFrontmatter(data)

	p := parser.New()
	root := p.Parse(data)
	var anchors []string

	ast.WalkFunc(root, func(node ast.Node, entering bool) ast.WalkStatus {
		// The callback function is called twice for every node: once with entering=true when the branch is
		// first visited, then with entering=false after all the children are done. We just need to validate
		// each node once.
		if !entering {
			return ast.GoToNext
		}

		// Hugo auto-generates anchors only from headings.
		heading, ok := node.(*ast.Heading)
		if !ok {
			return ast.GoToNext
		}

		// Build the heading text from children.
		text := ""

		for _, child := range heading.GetChildren() {
			if leaf := child.AsLeaf(); leaf != nil && len(leaf.Literal) > 0 {
				text = text + string(leaf.Literal) + " "
			}
		}

		if anchor := convertHeadingToAnchor(text); anchor != "" {
			anchors = append(anchors, anchor)
		}
		return ast.GoToNext
	})

	return anchors, nil
}

func convertHeadingToAnchor(heading string) string {
	anchor := strings.ToLower(heading)
	anchor = hugoAnchorCleanup.ReplaceAllString(anchor, "")
	anchor = hugoAnchorReplacement.ReplaceAllString(anchor, "-")
	anchor = strings.Trim(anchor, "- ")
	return anchor
}

func validateMarkdownFiles(docRoot string, anchors markdownAnchors) validationErrors {
	var errs validationErrors

	errs.add(filepath.Walk(docRoot, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) == ".md" {
			validationErrs := validateMarkdownFile(path, anchors)
			errs.add(validationErrs.errs()...)
		}

		return nil
	}))

	return errs
}

func validateMarkdownFile(file string, anchors markdownAnchors) validationErrors {
	data, err := os.ReadFile(file)
	if err != nil {
		return validationErrors{errors.Wrapf(err, "unable to read file %s", file)}
	}

	return validateMarkdownFileContent(file, data, anchors)
}

func validateMarkdownFileContent(sourceFile string, data []byte, anchors markdownAnchors) validationErrors {
	var errs validationErrors

	// Remove frontmatter because not supported by the parsing library.
	data = removeFrontmatter(data)

	p := parser.New()
	root := p.Parse(data)

	ast.WalkFunc(root, func(node ast.Node, entering bool) ast.WalkStatus {
		// The callback function is called twice for every node: once with entering=true when the branch is
		// first visited, then with entering=false after all the children are done. We just need to validate
		// each node once.
		if !entering {
			return ast.GoToNext
		}

		switch v := node.(type) {
		case *ast.Link:
			errs.add(validateMarkdownLink(sourceFile, string(v.Destination), anchors))
		case *ast.Image:
			errs.add(validateMarkdownImage(sourceFile, string(v.Destination)))
		}

		return ast.GoToNext
	})

	return errs
}

func validateMarkdownLink(sourceFile, link string, anchors markdownAnchors) error {
	// Check if it's a Hugo relref.
	if matches := hugoRelref.FindStringSubmatch(link); len(matches) > 0 {
		linkPath := matches[1]
		linkAnchor := matches[2]

		// Build the actual destination of the link (could be the page itself if it's just an anchor).
		destination := sourceFile
		if linkPath != "" {
			destination = filepath.Join(filepath.Dir(sourceFile), linkPath)
		}

		// Check if the linked document exists.
		if linkPath != "" {
			if _, err := os.Stat(destination); os.IsNotExist(err) {
				return newValidationError(sourceFile, errLinkNotExist, link)
			}
		}

		// Check if the linked anchor exists.
		if linkAnchor != "" {
			if err := anchors.exists(destination, linkAnchor); err != nil {
				return newValidationError(sourceFile, err.Error())
			}
		}

		return nil
	}

	// Check if it's an anchor link.
	if strings.HasPrefix(link, "#") {
		if err := anchors.exists(sourceFile, link[1:]); err != nil {
			return newValidationError(sourceFile, err.Error())
		}
		return nil
	}

	// Check if it's a valid URL.
	parsed, err := url.Parse(link)
	if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
		return nil
	}

	return newValidationError(sourceFile, errLinkInvalid, link)
}

func validateMarkdownImage(sourceFile, link string) error {
	// We enforce the use of Hugo bundles, so:
	// 1. All images should be places in the same directory.
	// 2. Only index.md or _index.md can link an image.
	if filepath.Clean(link) != filepath.Base(link) {
		return newValidationError(sourceFile, errImageLocatedOutside, link, filepath.Dir(sourceFile))
	}

	if filename := filepath.Base(sourceFile); filename != "index.md" && filename != "_index.md" {
		return newValidationError(sourceFile, errImageNotIndex, link, sourceFile)
	}

	// Ensure the image exists.
	if _, err := os.Stat(filepath.Join(filepath.Dir(sourceFile), link)); os.IsNotExist(err) {
		return newValidationError(sourceFile, errImageNotExist, link)
	}

	return nil
}

// removeFrontmatter removes the frontmatter from the markdown content.
func removeFrontmatter(data []byte) []byte {
	content := string(data)

	// Check if content has frontmatter.
	if !strings.HasPrefix(content, "---\n") {
		return data
	}

	// Look for the index of the closing frontmatter.
	closingIndex := strings.Index(content[4:], "---\n")
	if closingIndex == -1 {
		return data
	}

	return []byte(content[closingIndex+8:])
}

type validationError struct {
	sourceFile string
	message    string
}

func newValidationError(sourceFile, message string, args ...interface{}) validationError {
	return validationError{
		sourceFile: sourceFile,
		message:    fmt.Sprintf(message, args...),
	}
}

func (e validationError) Error() string {
	return fmt.Sprintf("Source: %s\nError:  %s", e.sourceFile, e.message)
}

type validationErrors []error

func (e *validationErrors) add(errs ...error) {
	for _, err := range errs {
		if err != nil {
			*e = append(*e, err)
		}
	}
}

func (e *validationErrors) errs() []error {
	if e == nil {
		return nil
	}

	return *e
}

type markdownAnchors map[string][]string

func (all markdownAnchors) add(file string, anchors ...string) {
	absPath := absPath(file)
	all[absPath] = append(all[absPath], anchors...)
}

func (all markdownAnchors) exists(file, anchor string) error {
	absPath := absPath(file)

	for _, actual := range all[absPath] {
		if anchor == actual {
			return nil
		}
	}

	return fmt.Errorf(errAnchorNotExist, anchor, file, strings.Join(all[absPath], ", "))
}

// absPath is a convenient utility to get the absolute path of a given filesystem path.
func absPath(path string) string {
	path, err := filepath.Abs(path)
	if err != nil {
		panic(fmt.Sprintf("unable to get absolute path of %q: %v", path, err))
	}

	return path
}
