// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/regexp"
)

var (
	// The license directive name.
	licenseDirective = "SPDX-License-Identifier"

	// The default license to apply.
	defaultLicense = "AGPL-3.0-only"

	// Maps regex matching the supported files with the comment style to use.
	supportedFiles = map[*regexp.Regexp]string{
		regexp.MustCompile(`\.go$`):      "//",
		regexp.MustCompile(`\.proto$`):   "//",
		regexp.MustCompile(`\.sh$`):      "#",
		regexp.MustCompile(`Dockerfile`): "#",
	}

	// Regex used to match the hashbang on the first line.
	hashBangRegex = regexp.MustCompile(`^#!.+\n`)
)

func isSupported(path string) (bool, string) {
	for re, commentStyle := range supportedFiles {
		if re.MatchString(path) {
			return true, commentStyle
		}
	}

	return false, ""
}

func addLicense(dir string) error {
	log.Printf("checking license header in %s\n", dir)

	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if info.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}

		// Add the license only to supported files.
		supported, commentStyle := isSupported(path)
		if !supported {
			return nil
		}

		// Skip generated Go files.
		if strings.HasSuffix(path, ".pb.go") || strings.HasSuffix(path, "_generated.go") {
			return nil
		}

		b, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return err
		}

		var bb bytes.Buffer

		// If the original file has an hashbang, we should preserve it on the first line.
		if match := hashBangRegex.FindString(string(b)); match != "" {
			_, _ = bb.WriteString(match)

			// Remove it from the original content.
			b = b[len(match):]
		}

		// Add the default license directory if none exists.
		if !strings.Contains(string(b), commentStyle+" "+licenseDirective+":") {
			log.Printf("adding the license directive to %s\n", path)
			_, _ = bb.WriteString(fmt.Sprintf("%s %s: %s\n", commentStyle, licenseDirective, defaultLicense))
		}

		// Add the provenance from Cortex for forked files. This has been run just once
		// after the fork.
		//if !strings.Contains(string(b), commentStyle+" Provenance-includes-location:") {
		//	log.Printf("adding default provenance directive to %s\n", path)
		//	_, _ = bb.WriteString(strings.Join([]string{
		//		commentStyle + " Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/" + path,
		//		commentStyle + " Provenance-includes-license: Apache-2.0",
		//		commentStyle + " Provenance-includes-copyright: The Cortex Authors.",
		//	}, "\n") + "\n")
		//}

		// We need to add a newline only if the next line is not another
		// comment or an empty line, otherwise we wanna keep all comments adjacent.
		if !strings.HasPrefix(string(b), commentStyle) && !strings.HasPrefix(string(b), "\n") {
			_, _ = bb.WriteString("\n")
		}

		// Add the rest of the file.
		_, _ = bb.Write(b)

		return os.WriteFile(path, bb.Bytes(), 0600)
	})
}

func main() {
	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: add-license <dir> ...")
		os.Exit(1)
	}

	for _, dir := range args {
		if err := addLicense(dir); err != nil {
			log.Fatal(err)
		}
	}
}
