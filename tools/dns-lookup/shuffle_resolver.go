package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"
)

func lookupHost(n int, name string, host string, lookup func(ctx context.Context, host string) ([]string, error)) (int, int) {
	var (
		startWith3, startWith18 int
	)
	for i := 1; i <= n; i++ {
		ips, err := lookup(context.Background(), host)
		if err != nil {
			panic(err)
		}
		if strings.HasPrefix(ips[0], "3") {
			startWith3++
		} else {
			startWith18++
		}
		if i%1000 == 0 {
			time.Sleep(30 * time.Second)
		}
		if i%500 == 0 {
			fmt.Printf("\ttest: %s, execution: %d out of %d\n", name, i, n)
		}
	}
	return startWith3, startWith18
}

type shuffleResolver struct {
	*net.Resolver
}

func shuffle[T any](slice []T) []T {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return slice
}

func (r *shuffleResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	ips, err := r.Resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	return shuffle(ips), nil
}

func main() {
	var (
		startWith3, startWith18 int
		start                   time.Time
		host                    = "test-cortex-dev-05-dev-us-east-0.grafana-dev.net"
		shuffle                 = &shuffleResolver{Resolver: net.DefaultResolver}
		def                     = net.DefaultResolver
	)
	runs := map[string]func(context.Context, string) ([]string, error){
		"DefaultResolver": def.LookupHost,
		"shuffleResolver": shuffle.LookupHost,
	}
	for name, resolver := range runs {
		start = time.Now()
		startWith3, startWith18 = lookupHost(5000, name, host, resolver)
		fmt.Println("resolver", name, "startWith3", startWith3, "startWith18", startWith18, "duration", time.Since(start))
	}
}
