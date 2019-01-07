#!/usr/bin/python3

import pickle
import json
import pandas

from hashlib import md5

from datetime import datetime
from os import stat

from flask import Flask, request
from flask_restful  import Resource, Api


column_types = {
    'grid_id':int,
    'violation_description':str,
    'dow':int,
    'year':int,
    'hour':int,
    'ward':int,
    'department_category':str,
    'ticket_queue':str,
    'is_business_district':bool,
    'current_amount_due':float,
    'total_payments':float,
    'penalty':float
}

parse_dates=['issue_date']
datas = pandas.read_csv('data/tickets.csv', dtype=column_types, parse_dates=parse_dates, infer_datetime_format=True)

with open('data/blank_grid_mercator.geojson', 'r') as fh:
    empty_grid_json = json.load(fh)

def geojson_from_df(df):
    grid_vals_dict = dict(df)

    new_feats = []

    for feature in empty_grid_json['features']:
        feat_props = feature['properties']
        grid_id = feat_props['ID']

        if 'geometry' in feature and not feature['geometry']:
            print('Geometry missing from ', grid_id)
            continue

        if grid_id in grid_vals_dict:
            data_val = int(grid_vals_dict[grid_id])
            if data_val <= 0:
                continue

            feature['properties']['data_val'] = data_val
        else:
            continue

        new_feats.append(feature)

    if not any(new_feats):
        new_feats.append(
    {
      "type": "Feature",
      "properties": {"data_val": 0},
      "geometry": {
        "type": "Polygon",
        "coordinates": [
[[[-9789724.66045665, 5107551.543942757],[-9742970.474323519, 5107551.543942757]],
 [[-9742970.474323519, 5164699.247119262],[-9789724.66045665, 5164699.247119262]]],
        ]
      }
    })


    ret_geojson = {
        'type': 'FeatureCollection',
        'crs': {'properties': {'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'}, 'type': 'name'},
        'features': new_feats
    }

    return ret_geojson

def get_data(violation_descriptions, dows, start_time, end_time, 
        start_hour, end_hour, wards, dept_categories, ticket_queues,
        include_cbd, agg_mode):

    #hash of dict = cache file location
    md5obj = md5()
    md5obj.update(str([violation_descriptions, dows, start_time,
        end_time, start_hour, end_hour, wards, dept_categories,
        ticket_queues, include_cbd, agg_mode]).encode('utf-8'))

    request_hash = md5obj.hexdigest()

    cached_geojson = get_cached_geojson(request_hash)

    if cached_geojson:
        return cached_geojson

    global datas
    new_datas = datas.copy()

    print(len(new_datas))

    if 'All' not in violation_descriptions:
        violation_descriptions = list(map(str.upper, violation_descriptions))
        print(violation_descriptions)
        new_datas = new_datas[new_datas.violation_description.isin(violation_descriptions)]


    if 'All' not in dows:
        new_datas = new_datas[new_datas.dow.isin(dows)]
    
    if start_hour != 0:
        print(type(start_hour), type(new_datas.hour))
        new_datas = new_datas[new_datas.hour >= start_hour]
    
    if end_hour != 23:
        new_datas = new_datas[new_datas.hour <= end_hour]
    
    if 'All' not in wards:
        new_datas = new_datas[new_datas.ward.isin(wards)]
    
    if 'All' not in dept_categories:
        new_datas = new_datas[new_datas.department_category.isin(dept_categories)]
    
    if 'All' not in ticket_queues:
        new_datas = new_datas[new_datas.ticket_queue.isin(ticket_queues)]
    
    if not include_cbd:
        new_datas = new_datas[new_datas.is_business_district != True]
   
    if start_time != datetime(2013, 1, 1):
        new_datas = new_datas[new_datas.issue_date >= start_time]

    if end_time != datetime(2018,5,14):
        new_datas = new_datas[new_datas.issue_date <= end_time]

    #agg_mode check must come last
    if agg_mode == 'count': 
        pre_geojson_df = new_datas.groupby('grid_id').size()

    if agg_mode == 'due': 
        pre_geojson_df = new_datas.groupby('grid_id').current_amount_due.sum()

    if agg_mode == 'paid':
        pre_geojson_df = new_datas.groupby('grid_id').total_payments.sum()

    if agg_mode == 'penalties': #must come last
        pre_geojson_df = new_datas.groupby('grid_id').penalty.sum()

    geojson_ret = geojson_from_df(pre_geojson_df)
    print(len(new_datas))

    cache_results(geojson_ret, request_hash)

    return geojson_ret

app = Flask(__name__)
api = Api(app)

def cache_results(geojson_data, filename, path='/opt/ticket_viz/cache'):
    with open('{}/{}'.format(path, filename), 'w') as fh:
        json.dump(geojson_data, fh)

def get_cached_geojson(filename, path='/opt/ticket_viz/cache'):
    try:
        with open('{}/{}'.format(path, filename)) as fh:
            return json.load(fh)
    except:
        return None

class GeoJsonEndpoint(Resource):
    def get(self):
        print(request.form['data'])


        req_json = json.loads(request.form['data'])

        violation_descriptions = req_json['violation_descriptions']
        dows = req_json['dows']
        start_time = datetime.strptime(req_json['start_time'], '%Y-%m-%d')
        end_time = datetime.strptime(req_json['end_time'], '%Y-%m-%d')
        start_hour = req_json['start_hour']
        end_hour = req_json['end_hour']
        wards = req_json['wards']
        dept_categories = req_json['dept_categories']
        ticket_queues = req_json['ticket_queues']
        include_cbd = req_json['include_cbd']
        agg_mode = req_json['agg_mode']

        ret_data = get_data(violation_descriptions, dows, start_time,
                       end_time, start_hour, end_hour, wards,
                       dept_categories, ticket_queues, 
                       include_cbd, agg_mode)


        return json.dumps(ret_data)

api.add_resource(GeoJsonEndpoint, '/')

if __name__ == '__main__':
    app.run()
