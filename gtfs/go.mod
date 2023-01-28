module github.com/gavincabbage/influxdb-gtfs/gtfs

go 1.19

require (
	github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs v0.0.0-20230119222401-871fbb548713
	github.com/golang/protobuf v1.5.2
	github.com/influxdata/line-protocol/v2 v2.2.1
	golang.org/x/sync v0.1.0
)

require google.golang.org/protobuf v1.26.0 // indirect
