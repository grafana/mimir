using Go = import "/go.capnp";
@0xc41513495eab566b;
$Go.package("client");
$Go.import("github.com/grafana/mimir/pkg/ingester/client");

struct CpnWriteRequest2 {
    timeseries @0 :List(TimeSeries2);
    source @1 :Source;
    metadata @2 :List(MetricMetadata2);
    skipLabelNameValidation @3 :Bool;

    enum Source {
        api @0;
        rule @1;
    }

    struct TimeSeries2 {
        labels @0 : Text;
        samples @1 : List(Sample2);

        struct Sample2 {
            timestampMs @0 : Int64;
            value @1 : Float64;
        }
    }

    struct MetricMetadata2 {
        type @0 : MetricType2;
        metricFamilyName @1 : Text;
        help @2 : Text;
        unit @3 : Text;

        enum MetricType2 {
            unknown        @0;
            counter        @1;
            gauge          @2;
            histogram      @3;
            gaugehistogram @4;
            summary        @5;
            info           @6;
            stateset       @7;
        }
    }
}
