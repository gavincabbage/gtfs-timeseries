from flightsql import connect, FlightSQLClient
from geojson import LineString,Feature,FeatureCollection,dump
import os
import psycopg


ndx_vehicle_id, ndx_time, ndx_route_id, ndx_latitude, ndx_longitude = 0, 1, 2, 3, 4


def format_query(table_name, interval):
    return '''
SELECT vehicle_id, time, route_id, latitude, longitude
FROM {table_name}
WHERE LOWER(route_id) IN ('blue', 'red', 'orange', 'green-b', 'green-c', 'green-d', 'green-e', 'mattapan')
AND time > now() - INTERVAL '{interval}'
GROUP BY vehicle_id, time, route_id, latitude, longitude
ORDER BY vehicle_id, time, route_id, latitude, longitude
'''.format(table_name=table_name, interval=interval)


def to_geojson(conn, query):
    cursor = conn.cursor()

    features = []
    coords = []
    current_vehicle = ''
    current_route = ''
    previous_coord = ()
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        if row[ndx_vehicle_id] != current_vehicle and current_vehicle != '':
            features.append(Feature(
                geometry=LineString(coords),
                properties={"vehicle_id":current_vehicle, "route_id":current_route},
            ))
            coords = []

        current_vehicle = str(row[ndx_vehicle_id])
        current_route = str(row[ndx_route_id])

        long, lat = float(row[ndx_longitude]), float(row[ndx_latitude])
        current_coord = (long, lat)
        if filter_outlier(previous_coord, current_coord):
            continue

        coords.append((
            long,
            lat,
            0,
            int(row[ndx_time].timestamp())
        ))
        previous_coord = current_coord

    features.append(Feature(
        geometry=LineString(coords),
        properties={"vehicle_id":current_vehicle},
    ))

    return FeatureCollection(features)


def filter_outlier(previous, current):
    threshold = 0.05
    if previous == ():
        return False

    return abs(current[0]-previous[0]) > threshold or abs(current[1]-previous[1]) > threshold


def run_flightsql(host, token, bucket):
    client = FlightSQLClient(
        host=host,
        port=443,
        token=token,
        metadata={"bucket-name": bucket},
    )
    conn = connect(client)

    out = to_geojson(conn, format_query('iox.position', '1 hour'))
    with open('mbta_flightsql.geojson', 'w') as f:
        dump(out, f)


def run_postgresql(dsn):
    conn = psycopg.connect(dsn)

    out = to_geojson(conn, format_query('position', '1 hour'))
    with open('mbta_postgresql.geojson', 'w') as f:
        dump(out, f)


if __name__ == '__main__':
    host = os.getenv('INFLUXDB_HOST')
    token = os.getenv('INFLUXDB_TOKEN')
    bucket = os.getenv('INFLUXDB_BUCKET')
    run_flightsql(host, token, bucket)

    dsn = os.getenv("TIMESCALE_DSN")
    run_postgresql(dsn)