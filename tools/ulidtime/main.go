package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oklog/ulid"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Usage:", os.Args[0], "[ulid ...]")
		return
	}

	for _, v := range os.Args[1:] {
		id, err := ulid.Parse(v)
		if err != nil {
			log.Printf("failed to parse %q: %v", v, err)
		} else {
			fmt.Println(ulid.Time(id.Time()).UTC().Format(time.RFC3339Nano))
		}
	}
}
