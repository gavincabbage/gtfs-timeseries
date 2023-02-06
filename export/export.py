from flightsql import connect, FlightSQLClient
from geojson import LineString,Feature,FeatureCollection,dump
import os


query = '''
SELECT vehicle_id, time, route_id, latitude, longitude
FROM iox.position
WHERE LOWER(route_id) IN ('blue', 'red', 'orange', 'green-b', 'green-c', 'green-d', 'green-e', 'mattapan')
AND time > now() - INTERVAL '1 hour'
GROUP BY vehicle_id, time, route_id, latitude, longitude
ORDER BY vehicle_id, time, route_id, latitude, longitude
'''

ndx_vehicle_id, ndx_time, ndx_route_id, ndx_latitude, ndx_longitude = 0, 1, 2, 3, 4


def to_geojson(conn):
    cursor = conn.cursor()

    features = []
    coords = []
    current_vehicle = ''
    current_route = ''
    previous_coord = ()
    for row in cursor.execute(query):
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


def run(host, token, bucket):
    client = FlightSQLClient(
        host=host,
        port=443,
        token=token,
        metadata={"bucket-name": bucket},
    )
    conn = connect(client)

    with open('mbta.geojson', 'w') as f:
        dump(to_geojson(conn), f)


if __name__ == '__main__':
    host = os.getenv('INFLUXDB_HOST')
    token = os.getenv('INFLUXDB_TOKEN')
    bucket = os.getenv('INFLUXDB_BUCKET')
    run(host, token, bucket)
