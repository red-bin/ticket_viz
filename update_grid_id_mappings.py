#!/usr/bin/python3

from shapely.geometry import Polygon, Point
import psycopg2
import geojson
import csv
from pprint import pprint

import viz_config as conf

geoj = geojson.load(open('data/grid_canvas_cropped.geojson','r'))

grid_centroids = {}

new_features = []

grid_polys = {}


crs = {"type": "name",
       "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84" }}


for feature in geoj['features']:
    if not feature['geometry']:
        continue
    geometry = feature['geometry']

    coords = geometry['coordinates']
    if len(coords[0]) < 4:
        continue

    poly = Polygon(coords[0])
    grid_polys[feature['properties']['ID']] = poly
  
    centroid_xy = poly.centroid.xy
    centroid_x = centroid_xy[0][0]
    centroid_y = centroid_xy[1][0]

    feature['properties']['CENTOID_LNG'] = centroid_x
    feature['properties']['CENTOID_LAT'] = centroid_y

    new_features.append(feature)

    grid_centroids[feature['properties']['ID']] = (centroid_x, centroid_y)

new_features_coll = geojson.FeatureCollection(new_features, crs=crs)

import viz_config as conf

def pg_conn():
     conn = psycopg2.connect(conf.connection_str)
     return conn

conn = pg_conn()
curs = conn.cursor()


sqlstr = "SELECT distinct(geocoded_address), geocoded_lng, geocoded_lat from tickets"
curs.execute(sqlstr)

geocoded_addrs = curs.fetchall()

potential_grid_addrs = dict([ (addr,[]) for addr,_,__ in  geocoded_addrs])
addr_points = dict([ (addr, Point(lng, lat)) for addr,lng,lat in  geocoded_addrs])

print("Finding close grid ids")

for grid_id, grid_centroid in grid_centroids.items():
    grid_lng, grid_lat = grid_centroid

    for address, geo_lng, geo_lat in geocoded_addrs:
        lng_diff = abs(grid_lng - geo_lng)
        lat_diff = abs(grid_lat - geo_lat)

        if lng_diff < .004 and lat_diff < .004:
            potential_grid_addrs[address].append(grid_id)

print("Finding addrs in grids")

addr_grids = dict([(i,[]) for i in potential_grid_addrs])
for addr, addr_point in addr_points.items():
    addr_grids[addr] = None

    for grid_id in potential_grid_addrs[addr]:
        grid_poly = grid_polys[grid_id]

        if grid_poly.contains(addr_point):
            addr_grids[addr] = grid_id


conn = pg_conn()
curs = conn.cursor()

curs.execute("CREATE TEMP TABLE grids (addr TEXT, grid_id INTEGER)")
for addr, grid_id in addr_grids.items():
    curs.execute("INSERT INTO grids VALUES (%s, %s)", (addr, grid_id))

curs.execute("""
UPDATE tickets SET grid_id = g.grid_id 
FROM grids g 
WHERE g.addr = tickets.geocoded_address ;""")

conn.commit()

print("Done! Check!")
