package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func main() {
	var port int
	flag.IntVar(&port, "port", 9095, "Port to port-forward.")

	var podTemplate string
	flag.StringVar(&podTemplate, "pod-template", "ingester-zone-<zone>-<id>", "Template to generate the pod ID in the format 'ingester-zone-<zone>-<id>'")

	var podMinID int
	var podMaxID int
	flag.IntVar(&podMinID, "pod-min-id", 0, "Min pod ID (included)")
	flag.IntVar(&podMaxID, "pod-max-id", 0, "Max pod ID (included)")

	var podZones flagext.StringSliceCSV
	flag.Var(&podZones, "pod-zones", "Comma-separated list of zones")

	var namespace string
	flag.StringVar(&namespace, "namespace", "", "Namespace of the pods.")

	var orgID string
	flag.StringVar(&orgID, "org", "", "Organization ID to query the ingester.")

	flag.Parse()

	if orgID == "" {
		log.Fatal("orgID is required")
	}

	if namespace == "" {
		log.Fatal("namespace is required")
	}

	// Generate pod IDs.
	pods := generatePodIDsWithinRange(podTemplate, podZones, podMinID, podMaxID)
	log.Println("Pods:", pods)

	ctx := user.InjectOrgID(context.Background(), orgID)
	process := func(pod string, localPort int) {
		addr := fmt.Sprintf("localhost:%d", localPort)

		err := pushSeriesToIngester(ctx, pod, addr)
		if err != nil {
			log.Printf("failed to check ingester %s: %v", pod, err)
		} else {
			log.Printf("Pushed to %s", pod)
		}
	}

	for _, pod := range pods {
		if err := processPortForwarded(pod, namespace, port, process); err != nil {
			log.Printf("Error processing pod %s in namespace %s: %v", pod, namespace, err)
		}
	}
}

func pushSeriesToIngester(ctx context.Context, pod, addr string) error {
	// To keep it simple, create a gRPC client each time.
	clientMetrics := ingester_client.NewMetrics(nil)
	clientConfig := ingester_client.Config{}
	flagext.DefaultValues(&clientConfig)

	client, err := ingester_client.MakeIngesterClient(ring.InstanceDesc{Addr: addr}, clientConfig, clientMetrics)
	if err != nil {
		return err
	}
	wr := mimirpb.NewWriteRequest(nil, mimirpb.API).
		AddFloatSeries([][]mimirpb.LabelAdapter{
			{
				{Name: "__name__", Value: "test_metric"},
				{Name: "sent_to_pod", Value: pod},
			},
		}, []mimirpb.Sample{
			{
				TimestampMs: time.Now().UnixMilli(), Value: 1.0,
			},
		}, []*mimirpb.Exemplar{
			nil,
		})

	_, err = client.Push(ctx, wr)
	return err
}
