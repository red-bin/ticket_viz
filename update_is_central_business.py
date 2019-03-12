#!/usr/bin/python3

import psycopg2cffi
import geojson
import csv
from shapely.geometry import Polygon, Point

from multiprocessing import Pool

import viz_config as conf

def pg_conn():
     conn = psycopg2cffi.connect(conf.connection_str)
     return conn

sqlstr = """
SELECT DISTINCT(geocoded_address), geocoded_lng, geocoded_lat FROM tickets 
WHERE geocoded_lng > -87.660093 
AND geocoded_lat > 41.859828"""

cbd_fn = 'Boundaries - Central Business District.geojson'
cbd_fp = '/opt/data/shapefiles/central_business_district/{}'.format(cbd_fn)
cbd_json = geojson.load(open(cbd_fp, 'r'))

cbd_poly = Polygon(cbd_json['features'][0]['geometry']['coordinates'][0][0])

conn = pg_conn()
curs = conn.cursor()

curs.execute(sqlstr)

geocoded_addrs = curs.fetchall()

addr_points = []
for addr, lng, lat in geocoded_addrs:
    addr_points.append((addr, Point(float(lng), float(lat))))

inside_cbd = []
for addr, addr_point in addr_points:
    if cbd_poly.contains(addr_point):
        inside_cbd.append([addr])

curs.execute("CREATE TEMP TABLE cbd (addr TEXT)")
curs.executemany("INSERT INTO cbd VALUES (%s)", inside_cbd)

curs.execute("update tickets set is_business_district = True from cbd c where c.addr = tickets.geocoded_address")
curs.execute("update tickets set is_business_district = False where is_business_district IS NOT True")

conn.commit()
