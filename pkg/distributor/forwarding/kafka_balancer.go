package forwarding

// kafkaBalancer is a load balancer for kafka which distributes the messages in such a way that
// samples which should be aggregated together get written into the same partition.
type kafkaBalancer struct{}
