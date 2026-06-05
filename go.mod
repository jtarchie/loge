module github.com/jtarchie/loge

go 1.25.0

require (
	github.com/FastFilter/xorfilter v0.5.1
	github.com/SaveTheRbtz/zstd-seekable-format-go/pkg v0.10.0
	github.com/alecthomas/kong v1.15.0
	github.com/aws/aws-sdk-go-v2 v1.41.12
	github.com/aws/aws-sdk-go-v2/config v1.32.23
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.22.25
	github.com/aws/aws-sdk-go-v2/service/s3 v1.103.2
	github.com/fsnotify/fsnotify v1.10.1
	github.com/georgysavva/scany/v2 v2.1.4
	github.com/goccy/go-json v0.10.6
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/imroc/req/v3 v3.57.0
	github.com/jaswdr/faker/v2 v2.9.1
	github.com/jtarchie/sqlitezstd v0.0.0-20260605152647-568da00f8469
	github.com/jtarchie/worker v0.0.0-20251226174303-31967c3fe3c0
	github.com/klauspost/compress v1.18.6
	github.com/labstack/echo/v5 v5.1.1
	github.com/mattn/go-sqlite3 v1.14.45
	github.com/onsi/ginkgo/v2 v2.29.0
	github.com/onsi/gomega v1.41.0
	github.com/phayes/freeport v0.0.0-20220201140144-74d24b5ae9f5
	github.com/samber/lo v1.53.0
	github.com/tinylib/msgp v1.6.4
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.13 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.22 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.1.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.31.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.36.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.43.2 // indirect
	github.com/aws/smithy-go v1.27.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-querystring v1.2.0 // indirect
	github.com/google/pprof v0.0.0-20260604005048-7023385849c0 // indirect
	github.com/icholy/digest v1.1.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/psanford/httpreadat v0.1.0 // indirect
	github.com/psanford/sqlite3vfs v0.0.0-20260519004904-f9180fa2acc9 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.57.1 // indirect
	github.com/refraction-networking/utls v1.8.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/mod v0.36.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.45.0 // indirect
)

replace github.com/jtarchie/worker => ./worker
