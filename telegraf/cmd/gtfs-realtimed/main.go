package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/golang/protobuf/proto"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"golang.org/x/sync/errgroup"
)

var argv struct {
	key      string
	username string
	password string

	tripUpdatesURL            string
	serviceAlertsURL          string
	vehiclePositionsURL       string
	vehiclePositionIncludeBus bool
	timeout                   time.Duration
}

func init() {
	flag.StringVar(&argv.key, "key", "", "GTFS API Key")
	flag.StringVar(&argv.username, "username", "", "GTFS username for Basic auth")
	flag.StringVar(&argv.password, "password", "", "GTFS password for Basic auth")

	flag.StringVar(&argv.tripUpdatesURL, "trip-updates-url", "https://host.test/TripUpdates.pb", "URL for trip updates")
	flag.StringVar(&argv.serviceAlertsURL, "service-alerts-url", "https://host.test/ServiceAlerts.pb", "URL for service alerts")
	flag.StringVar(&argv.vehiclePositionsURL, "vehicle-positions-url", "https://host.test/VehiclePositions.pb", "URL for vehicle positions")
	flag.BoolVar(&argv.vehiclePositionIncludeBus, "vehicle-positions-include-bus", false, "Include busses when collecting vehicle positions")
	flag.DurationVar(&argv.timeout, "request-timeout", 10*time.Second, "GTFS request timeout")
}

func main() {
	flag.Parse()

	ctx := context.Background()
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	c := &collector{
		vehiclePositionsIncludeBus: argv.vehiclePositionIncludeBus,
		client: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
			Timeout: argv.timeout,
		},
	}

	if argv.tripUpdatesURL == "" && argv.serviceAlertsURL == "" && argv.vehiclePositionsURL == "" {
		return errors.New("no source urls configured; at least one of vehicle-positions-url, trip-updates-url or service-alerts-url must be set")
	}

	if argv.tripUpdatesURL != "" {
		r, err := newRequest(ctx, argv.tripUpdatesURL)
		if err != nil {
			return err
		}
		c.tripUpdatesReq = r
	}

	if argv.serviceAlertsURL != "" {
		r, err := newRequest(ctx, argv.serviceAlertsURL)
		if err != nil {
			return err
		}
		c.serviceAlertsReq = r
	}

	if argv.vehiclePositionsURL != "" {
		r, err := newRequest(ctx, argv.vehiclePositionsURL)
		if err != nil {
			return err
		}
		c.vehiclePositionsReq = r
	}

	tick := make(chan os.Signal, 1)
	signal.Notify(tick, syscall.SIGUSR1)

	for {
		<-tick
		if err := c.run(); err != nil {
			fmt.Fprintf(os.Stderr, "run error: %v", err)
		}
	}
}

func newRequest(ctx context.Context, s string) (*http.Request, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	if argv.key != "" {
		q := u.Query()
		q.Set("key", argv.key)
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	if argv.username != "" && argv.password != "" {
		req.SetBasicAuth(argv.username, argv.password)
	}

	return req.WithContext(ctx), nil
}

type collector struct {
	client                     *http.Client
	tripUpdatesReq             *http.Request
	serviceAlertsReq           *http.Request
	vehiclePositionsReq        *http.Request
	vehiclePositionsIncludeBus bool
}

func (c *collector) run() error {
	var (
		g   errgroup.Group
		now = time.Now()
	)

	if c.tripUpdatesReq != nil {
		g.Go(func() error { return c.gatherTripUpdates(now) })
	}

	if c.serviceAlertsReq != nil {
		g.Go(func() error { return c.gatherServiceAlerts(now) })
	}

	if c.vehiclePositionsReq != nil {
		g.Go(func() error { return c.gatherVehiclePositions(now) })
	}

	return g.Wait()
}

func (c *collector) gatherTripUpdates(now time.Time) error {
	return nil
}

func (c *collector) gatherServiceAlerts(now time.Time) error {
	return nil
}

func (c *collector) gatherVehiclePositions(now time.Time) error {
	entities, err := c.gather(c.vehiclePositionsReq)
	if err != nil {
		return err
	}

	for _, entity := range entities {
		if c.shouldSkipVehiclePosition(entity) {
			continue
		}
		v := entity.Vehicle
		if v.Timestamp == nil {
			n := uint64(now.Unix())
			v.Timestamp = &n
		}

		var (
			fields = make(map[string]interface{})
			tags   = make(map[string]string)
		)

		if v.StopId != nil {
			tags["stop_id"] = *v.StopId
		}
		if v.Trip.DirectionId != nil {
			tags["direction_id"] = fmt.Sprintf("%d", *v.Trip.DirectionId)
		}

		if v.Position != nil {
			fields["latitude"] = v.Position.Latitude
			fields["longitude"] = v.Position.Longitude
			fields["bearing"] = v.Position.Bearing
			fields["odometer"] = v.Position.Odometer
			fields["speed"] = v.Position.Speed
		}

		if v.Vehicle != nil {
			tags["vehicle_id"] = *v.Vehicle.Id
			tags["vehicle_label"] = *v.Vehicle.Label
		}

		if v.Trip != nil {
			fields["trip_id"] = v.Trip.TripId
			if v.Trip.RouteId != nil {
				tags["route_id"] = *v.Trip.RouteId
			}
		}

		if v.CurrentStatus != nil {
			tags["current_status"] = v.CurrentStatus.String()
		}

		if v.CongestionLevel != nil {
			tags["congestion"] = v.CongestionLevel.String()
		}

		s, err := lp("position", fields, tags, time.Unix(int64(*v.Timestamp), 0))
		if err != nil {
			return fmt.Errorf("encoding line protocol: %v", err)
		}
		fmt.Println(s)
	}

	return nil
}

// TODO(gavincabbage): this is specific to the MBTA; need to allow for a regex for skipping (line/route ID too?)
// shouldSkipVehiclePosition returns true if either the entity is not a vehicle position
// update or it is a bus position update but busses are not configured to be included.
func (c *collector) shouldSkipVehiclePosition(entity *gtfs.FeedEntity) bool {
	if entity == nil || entity.Vehicle == nil {
		return true
	}

	const maxBusIDLength = 5

	return !c.vehiclePositionsIncludeBus &&
		entity.Vehicle.Vehicle != nil &&
		entity.Vehicle.Vehicle.Id != nil &&
		strings.HasPrefix(*entity.Vehicle.Vehicle.Id, "y") &&
		len(*entity.Vehicle.Vehicle.Id) <= maxBusIDLength
}

func (c *collector) gather(req *http.Request) ([]*gtfs.FeedEntity, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var feed gtfs.FeedMessage
	err = proto.Unmarshal(body, &feed)
	if err != nil {
		return nil, err
	}

	return feed.Entity, nil
}

func lp(measurement string, fields map[string]interface{}, tags map[string]string, t time.Time) (string, error) {
	var enc lineprotocol.Encoder
	enc.SetPrecision(lineprotocol.Nanosecond)
	enc.StartLine(measurement)
	var tagKeys []string
	for k, _ := range tags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys) // tag keys must be added in lexical order per AddTag docs
	for _, k := range tagKeys {
		enc.AddTag(k, tags[k])
	}
	for k, v := range fields {
		val, ok := newValue(v)
		if ok {
			enc.AddField(k, val)
		}
	}
	enc.EndLine(t)
	if err := enc.Err(); err != nil {
		return "", err
	}
	return string(enc.Bytes()), nil
}

// TODO(gavincabbage): better way? use reflect?
func newValue(x interface{}) (lineprotocol.Value, bool) {
	switch x := x.(type) {
	case *int64:
		if x == nil {
			break
		}
		return lineprotocol.IntValue(*x), true
	case *uint64:
		if x == nil {
			break
		}
		return lineprotocol.UintValue(*x), true
	case *float64:
		if x == nil {
			break
		}
		return lineprotocol.FloatValue(*x)
	case *float32:
		if x == nil {
			break
		}
		return lineprotocol.FloatValue(float64(*x))
	case *bool:
		if x == nil {
			break
		}
		return lineprotocol.BoolValue(*x), true
	case *string:
		if x == nil {
			break
		}
		return lineprotocol.StringValue(*x)
	default:
		return lineprotocol.NewValue(x)
	}
	return lineprotocol.Value{}, false
}
