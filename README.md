# influxdb-gtfs

Playing with (GTFS-realtime](https://developers.google.com/transit/gtfs-realtime) data in [InfluxDB Cloud](https://cloud2.influxdata.com/signup) using the new [IOx storage engine](https://github.com/influxdata/influxdb_iox).

## Contents

- `telegraf/`
    - A Telegraf `execd` plugin written in Go that polls GTFS-realtime data
- `export/`
    - Python code to query GTFS-realtime data in InfluxDB IOx via FlightSQL and convert it to GeoJSON
- `config.toml`
    - Telegraf configuration using the GTFS-realtime plugin in this repo to write to InfluxDB
- `keplergl.ipynb`
    - Juypter Notebook using KeplerGL to visualize data exported with the Python code in this repo
    
## Resource

Links I used while putting this together:
- [Visualizing Bus Trajectories in Denver](https://towardsdatascience.com/visualizing-bus-trajectories-in-denver-85ff02f3a746)
- [Visualizing Istanbul Bus Traffic With Python and KeplerGL](https://medium.com/swlh/visualizing-istanbul-bus-traffic-with-python-and-keplergl-a84895788825)
- [Using KeplerGL in JupyterLab](https://docs.kepler.gl/docs/keplergl-jupyter#keplergl)
