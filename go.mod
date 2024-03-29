module github.com/andrewchambers/hafs

go 1.17

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20221108203244-b4bd84bd1985
	github.com/cheynewallace/tabby v1.1.1
	github.com/detailyang/fastrand-go v0.0.0-20191106153122-53093851e761
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/minio/minio-go/v7 v7.0.39
	github.com/valyala/fastjson v1.6.3
	golang.org/x/sync v0.0.0-20220929204114-8fcdb60fdcc0
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875
)

require (
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/klauspost/cpuid/v2 v2.1.0 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/stretchr/testify v1.8.0 // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543 // indirect
	gopkg.in/ini.v1 v1.66.6 // indirect
)

replace github.com/hanwen/go-fuse/v2 => github.com/andrewchambers/go-fuse/v2 v2.0.0-20230121043514-3c9647baf8ee
