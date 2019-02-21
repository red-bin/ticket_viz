#!/usr/bin/python3

import geojson
from geopy.distance import geodesic
import psycopg2
import seaborn as sns

def pg_conn():
    connstr = "dbname=tickets host=localhost user=sdjfklsjga password=sdfjksldfja"
    conn = psycopg2.connect(connstr)

    return conn

conn = pg_conn()
curs = conn.cursor()

sqlstr = """
    SELECT t.officer, t.geocoded_lng, t.geocoded_lat, issue_date
    FROM tickets t
    WHERE issue_date >= '2012-01-01' 
    AND issue_date < '2012-12-31'
    ORDER BY t.officer, issue_date desc"""


curs.execute(sqlstr)

officers = {}

for officer, lng, lat, dt in curs.fetchall():
    if officer not in officers:
        officers[officer] = []

    officers[officer].append((lng, lat, dt))

officer_paths = dict([(o,[]) for o in officers.keys()])

for officer, vals in officers.items():
    print('====', officer, '====')
    last_lng = None
    last_lat = None
    last_dt = None
    current_path = []

    for lng, lat, dt in vals:

        if not lng or not lng:
            continue

        if not last_lng or not last_lat or not last_dt:
            #current_path.append((lng, lat)) 
            last_lng = lng
            last_lat = lat
            last_dt = dt

            continue

        distance_delta = geodesic((lng, lat), (last_lng, last_lat))
        distance_delta = distance_delta.meters

        dt_diff = dt - last_dt 
        dt_diff = abs(dt_diff.total_seconds())

        if distance_delta == 0.0 or dt_diff == 0:
            last_lng = lng
            last_lat = lat
            last_dt = dt

            continue

        #print()
        #print(distance_delta, dt_diff)
        walking_speed = distance_delta / dt_diff
        if walking_speed < 1.5 and dt_diff < 1200 and distance_delta <400:
        #    print("distance", distance_delta)
        #    print("walking speed", walking_speed)
            current_path.append((lng, lat))

        else:
            if len(current_path) > 1:
                officer_paths[officer].append(current_path)

            current_path = []
   
        last_lng = lng
        last_lat = lat
        last_dt = dt

    if len(current_path) > 1:
        officer_paths[officer].append(current_path)


def save_many(paths):
    for officer, geoms in paths.items():
        if not geoms:
            continue
        features = []

        for geom in geoms:
            ls = geojson.LineString(geom)
            feature = geojson.Feature(geometry=ls)
            features.append(feature)
    
        fh = open('/tmp/geojsons/test.%s.geojson' % officer, 'w')
        results = geojson.FeatureCollection(features)
        geojson.dump(results, fh)
        fh.close()

def save_one(paths):
    features = []
    for officer, geoms in paths.items():
        if not geoms:
            continue
    
        for geom in geoms:
            ls = geojson.LineString(geom)
            feature = geojson.Feature(geometry=ls)
            features.append(feature)
    
    fh = open('/tmp/geojsons/test.geojson', 'w')
    results = geojson.FeatureCollection(features)
    geojson.dump(results, fh)
    fh.close()

save_one(officer_paths)
