package main

import (
	"os"

	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func main() {
	nm := os.Args[1]
	b, err := os.ReadFile(nm)
	if err != nil {
		panic(err)
	}
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(b); err != nil {
		panic(err)
	}
	if jb, err := req.MarshalJSON(); err != nil {
		panic(err)
	} else {
		os.Stdout.Write(jb)
	}
}
