module github.com/grafana/mimir

go 1.25.5

require (
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.4
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b
	github.com/dustin/go-humanize v1.0.1
	github.com/edsrzf/mmap-go v1.2.0
	github.com/failsafe-go/failsafe-go v0.9.5
	github.com/go-kit/log v0.2.1
	github.com/go-openapi/strfmt v0.25.0
	github.com/go-openapi/swag v0.25.4 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.1
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v1.0.0
	github.com/google/gopacket v1.1.19
	github.com/gorilla/mux v1.8.1
	github.com/grafana/dskit v0.0.0-20260130082136-3948c2561458
	github.com/grafana/e2e v0.1.2-0.20251205060319-9884d3ffb1bf
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/influxdata/influxdb/v2 v2.8.0
	github.com/json-iterator/go v1.1.12
	github.com/minio/minio-go/v7 v7.0.98
	github.com/mitchellh/go-wordwrap v1.0.1
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opentracing-contrib/go-grpc v0.1.2
	github.com/opentracing-contrib/go-stdlib v1.1.1 // indirect
	github.com/opentracing/opentracing-go v1.2.1-0.20220228012449-10b1cf09e00b
	github.com/pkg/errors v0.9.1
	github.com/prometheus/alertmanager v0.30.0
	github.com/prometheus/client_golang v1.23.3-0.20260108101519-fb0838f53562
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.67.5
	github.com/prometheus/prometheus v1.99.0
	github.com/segmentio/fasthash v1.0.3
	github.com/spf13/afero v1.15.0
	github.com/stretchr/testify v1.11.1
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	go.uber.org/atomic v1.11.0
	go.uber.org/goleak v1.3.0
	golang.org/x/crypto v0.47.0
	golang.org/x/net v0.49.0
	golang.org/x/sync v0.19.0
	golang.org/x/time v0.14.0
	google.golang.org/grpc v1.78.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go/storage v1.59.1
	github.com/alecthomas/chroma/v2 v2.23.1
	github.com/alecthomas/kingpin/v2 v2.4.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.95.1
	github.com/cortexproject/promqlsmith v0.0.0-20251018201159-9e00e5e62e4e
	github.com/dennwc/varint v1.0.0
	github.com/felixge/fgprof v0.9.5
	github.com/go-openapi/swag/jsonutils v0.25.4
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8
	github.com/google/go-cmp v0.7.0
	github.com/google/go-github/v81 v81.0.0
	github.com/google/uuid v1.6.0
	github.com/grafana-tools/sdk v0.0.0-20220919052116-6562121319fc
	github.com/grafana/alerting v0.0.0-20251002141545-d513d62d3210
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/hashicorp/vault/api v1.22.0
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822
	github.com/oklog/ulid/v2 v2.1.1
	github.com/okzk/sdnotify v0.0.0-20240725214427-1c1fdd37c5ac
	github.com/pierrec/lz4/v4 v4.1.25
	github.com/prometheus/otlptranslator v1.0.0
	github.com/prometheus/procfs v0.19.2
	github.com/shirou/gopsutil/v4 v4.25.11
	github.com/spf13/pflag v1.0.10
	github.com/thanos-io/objstore v0.0.0-20250813080715-4e5fd4289b50
	github.com/tjhop/slog-gokit v0.1.5
	github.com/twmb/franz-go v1.20.6
	github.com/twmb/franz-go/pkg/kadm v1.17.2-0.20251227070528-0c71f7e25fa1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260121195810-e0832fcbdccb
	github.com/twmb/franz-go/pkg/kmsg v1.12.1-0.20251024215757-aea970d4d0d2
	github.com/twmb/franz-go/plugin/kotel v1.6.0
	github.com/twmb/franz-go/plugin/kprom v1.3.0
	github.com/tylertreat/BoomFilters v0.0.0-20251117164519-53813c36cc1b
	github.com/xlab/treeprint v1.2.0
	go.opentelemetry.io/collector/pdata v1.50.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.64.0
	go.opentelemetry.io/contrib/propagators/jaeger v1.39.0
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/sdk v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
	go.opentelemetry.io/proto/otlp v1.9.0
	go.uber.org/multierr v1.11.0
	golang.org/x/term v0.39.0
	google.golang.org/api v0.260.0
	google.golang.org/protobuf v1.36.11
	sigs.k8s.io/kustomize/kyaml v0.21.0
)

require (
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go/auth v0.18.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/monitoring v1.24.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.54.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.54.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.2.1 // indirect
	github.com/at-wat/mqtt-go v0.19.4 // indirect
	github.com/aws/aws-sdk-go v1.55.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.12 // indirect
	github.com/bboreham/go-loser v0.0.0-20230920113527-fcc2c21820a3 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cncf/xds/go v0.0.0-20251022180443-0feb69152e9f // indirect
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.4 // indirect
	github.com/go-openapi/swag/conv v0.25.4 // indirect
	github.com/go-openapi/swag/fileutils v0.25.4 // indirect
	github.com/go-openapi/swag/jsonname v0.25.4 // indirect
	github.com/go-openapi/swag/loading v0.25.4 // indirect
	github.com/go-openapi/swag/mangling v0.25.4 // indirect
	github.com/go-openapi/swag/netutils v0.25.4 // indirect
	github.com/go-openapi/swag/stringutils v0.25.4 // indirect
	github.com/go-openapi/swag/typeutils v0.25.4 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.4 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20260111202518-71be6bfdd440 // indirect
	github.com/grafana/otel-profiling-go v0.5.1 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.9 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.8 // indirect
	github.com/hashicorp/go-secure-stdlib/parseutil v0.2.0 // indirect
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.2 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/hashicorp/hcl v1.0.1-vault-7 // indirect
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/influxdata/tdigest v0.0.2-0.20210216194612-fc98d27c9e8b // indirect
	github.com/jaegertracing/jaeger-idl v0.6.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/mdlayher/vsock v1.2.1 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics v0.142.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.142.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor v0.142.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pires/go-proxyproto v0.8.1 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang/exp v0.0.0-20260101091701-2cd067eb23c9 // indirect
	github.com/prometheus/sigv4 v0.3.0 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/sercand/kuberesolver/v6 v6.0.1 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/tinylib/msgp v1.6.1 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/component v1.48.0 // indirect
	go.opentelemetry.io/collector/confmap v1.48.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.142.0 // indirect
	go.opentelemetry.io/collector/consumer v1.48.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.50.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.48.0 // indirect
	go.opentelemetry.io/collector/processor v1.48.0 // indirect
	go.opentelemetry.io/contrib/bridges/prometheus v0.64.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.38.0 // indirect
	go.opentelemetry.io/contrib/exporters/autoexport v0.64.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace v0.64.0 // indirect
	go.opentelemetry.io/contrib/samplers/jaegerremote v0.33.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.15.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.15.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.61.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.15.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.39.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.39.0 // indirect
	go.opentelemetry.io/otel/log v0.15.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.15.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/tools/godoc v0.1.0-deprecated // indirect
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/mail.v2 v2.3.1 // indirect
	gopkg.in/telebot.v3 v3.2.1 // indirect
	k8s.io/apimachinery v0.34.3 // indirect
	k8s.io/client-go v0.34.3 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.3 // indirect
	github.com/DmitriyVTitov/size v1.5.0
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.1 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.6 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.5 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.6.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/docker/go-connections v0.4.1-0.20210727194412-58542c764a11 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/efficientgo/core v1.0.0-rc.3
	github.com/efficientgo/e2e v0.13.1-0.20220923082810-8fa9daa8af8a // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/go-logfmt/logfmt v0.6.1
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/analysis v0.24.1 // indirect
	github.com/go-openapi/errors v0.22.4 // indirect
	github.com/go-openapi/jsonpointer v0.22.1 // indirect
	github.com/go-openapi/jsonreference v0.21.3 // indirect
	github.com/go-openapi/loads v0.23.2 // indirect
	github.com/go-openapi/runtime v0.27.1 // indirect
	github.com/go-openapi/spec v0.22.1 // indirect
	github.com/go-openapi/validate v0.25.1 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.9 // indirect
	github.com/googleapis/gax-go/v2 v2.16.0 // indirect
	github.com/gosimple/slug v1.1.1 // indirect
	github.com/grafana/gomemcache v0.0.0-20251127154401-74f93547077b
	github.com/hashicorp/consul/api v1.32.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.7 // indirect
	github.com/hashicorp/memberlist v0.5.3 // indirect
	github.com/hashicorp/serf v0.10.2 // indirect
	github.com/hashicorp/vault/api/auth/approle v0.11.0
	github.com/hashicorp/vault/api/auth/kubernetes v0.10.0
	github.com/hashicorp/vault/api/auth/userpass v0.11.0
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/compress v1.18.3
	github.com/klauspost/cpuid/v2 v2.2.11 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4
	github.com/miekg/dns v1.1.69 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/monochromegane/go-gitignore v0.0.0-20200626010858-205db1a8cc00 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/ncw/swift v1.0.53 // indirect
	github.com/oklog/run v1.2.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/common/sigv4 v0.1.0 // indirect
	github.com/prometheus/exporter-toolkit v0.15.1 // indirect
	github.com/rainycape/unidecode v0.0.0-20150907023854-cb7f23ec59be // indirect
	github.com/rs/cors v1.11.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/shurcooL/httpfs v0.0.0-20230704072500-f1e31cf0ba5c // indirect
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/spf13/cobra v1.9.1 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	go.etcd.io/etcd/api/v3 v3.6.7 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.7 // indirect
	go.etcd.io/etcd/client/v3 v3.6.7 // indirect
	go.mongodb.org/mongo-driver v1.17.6 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.64.0
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	golang.org/x/tools v0.41.0
	google.golang.org/genproto v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251222181119-0a764e51fe1b // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251222181119-0a764e51fe1b
	k8s.io/kube-openapi v0.0.0-20250710124328-f3f2b991d03b // indirect
	k8s.io/utils v0.0.0-20250604170112-4c0f3b243397 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)

replace github.com/prometheus/prometheus => github.com/grafana/mimir-prometheus v1.8.2-0.20260121014331-78e801ba42c3

// Replace memberlist with our fork which includes some changes that haven't been
// merged upstream yet for years and we don't expect to change anytime soon.
replace github.com/hashicorp/memberlist => github.com/grafana/memberlist v0.3.1-0.20251126142931-6f9f62ab6f86

// gopkg.in/yaml.v3
// + https://github.com/go-yaml/yaml/pull/691
// + https://github.com/go-yaml/yaml/pull/876
replace gopkg.in/yaml.v3 => github.com/colega/go-yaml-yaml v0.0.0-20220720105220-255a8d16d094

// We are using our modified version of the upstream GO regexp (branch remotes/origin/speedup)
replace github.com/grafana/regexp => github.com/grafana/regexp v0.0.0-20250905101755-5eb4f3acbf71

// Replace goautoneg with a fork until https://github.com/munnerz/goautoneg/pull/6 is merged
replace github.com/munnerz/goautoneg => github.com/grafana/goautoneg v0.0.0-20240607115440-f335c04c58ce

// Replace opentracing-contrib/go-stdlib with a fork until https://github.com/opentracing-contrib/go-stdlib/pull/68 is merged.
replace github.com/opentracing-contrib/go-stdlib => github.com/grafana/opentracing-contrib-go-stdlib v0.0.0-20230509071955-f410e79da956

// Replace opentracing-contrib/go-grpc with a fork until https://github.com/opentracing-contrib/go-grpc/pull/16 is merged.
replace github.com/opentracing-contrib/go-grpc => github.com/charleskorn/go-grpc v0.0.0-20231024023642-e9298576254f

// Replacing prometheus/alertmanager with our fork.
replace github.com/prometheus/alertmanager => github.com/grafana/prometheus-alertmanager v0.25.1-0.20250911094103-5456b6e45604

// Use Mimir fork of prometheus/otlptranslator to allow for higher velocity of upstream development,
// while allowing Mimir to move at a more conservative pace.
replace github.com/prometheus/otlptranslator => github.com/grafana/mimir-otlptranslator v0.0.0-20251017074411-ea1e8f863e1d
