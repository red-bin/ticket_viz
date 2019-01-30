#!/usr/bin/python3

import csv
import json
import pandas

from hashlib import md5

import pickle

from datetime import datetime
from os import stat

from flask import Flask, request
from flask_restful  import Resource, Api

import viz_config

column_types = {
    'grid_id':int,
    'violation_description':str,
    'dow':int,
    'year':int,
    'hour':int,
    'ward':str,
    'department_category':str,
    'ticket_queue':str,
    'is_business_district':bool,
    'current_amount_due':float,
    'total_payments':float,
    'penalty':float
}

parse_dates=['issue_date']

datas = pandas.read_csv(viz_config.tickets_csv, dtype=column_types, 
    parse_dates=parse_dates, infer_datetime_format=True)

pop_ratios = {}
with open(viz_config.census_csv,'r') as fh:
    reader = csv.DictReader(fh)

    for line in reader:
        grid_id = int(line['ID'])
        black_pop = float(line['BLACK_OR_AFRICAN_AMERICAN'])
        total_pop = float(line['TOTAL_POPULATION'])

        if total_pop == 0:
            pop_ratios[grid_id] = 0
            continue

        ratio = black_pop / total_pop 

        pop_ratios[grid_id] = ratio

with open(viz_config.empty_grid_geojson, 'r') as fh:
    empty_grid_json = json.load(fh)

test_gridvals = {}

def geojson_from_df(df):
    grid_vals_dict = df.to_dict()

    test_gridvals = grid_vals_dict
    new_feats = []

    for feature in empty_grid_json['features']:
        feat_props = feature['properties']
        grid_id = feat_props['ID']

        if 'geometry' in feature and not feature['geometry']:
            print('Geometry missing from ', grid_id)
            continue

        if grid_id in grid_vals_dict:
            data_val = grid_vals_dict[grid_id]
            if data_val <= 0.00000000:
                continue

            feature['properties']['data_val'] = float(data_val)
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

def timeseries_from_df(df, agg_mode, field_by, is_cumm=True):
    key_data = {}
    cumm_dict = {}
    for time, chunk_df in df.groupby('issue_date'): 
        if agg_mode == 'count':
            chunk_data = chunk_df.groupby(field_by).current_amount_due.size()
        elif agg_mode == 'due':
            chunk_data = chunk_df.groupby(field_by).current_amount_due.sum()
        elif agg_mode == 'paid':
            chunk_data = chunk_df.groupby(field_by).total_payments.sum()
        elif agg_mode == 'penalties':
            chunk_data = chunk_df.groupby(field_by).penalty.sum()

        for key, val in dict(chunk_data).items():
            if key not in key_data.keys():
                key_data[key] = {'xs':[], 'ys':[]}

            if is_cumm and key_data[key]['xs']:
                val = val + key_data[key]['ys'][-1]

            key_data[key]['xs'].append(time.strftime('%Y-%m-%d'))
            key_data[key]['ys'].append(float(val))

    ret_dict = {'keys':[], 'xs':[], 'ys':[]}
    for key, vals in key_data.items():
        ret_dict['keys'].append(key)
        ret_dict['xs'].append(vals['xs'])
        ret_dict['ys'].append(vals['ys'])

    return ret_dict

def normalize_geojson_data(df, normalize_by):
    print("normalize by:", normalize_by)
    total = df.sum()
    data_dict = dict(df)

    vals_total = 0
    for grid_id, data_val in data_dict.items():
        ratio = data_val / total

        if normalize_by == 'total_population':
            normalize_ratio = pop_ratios[grid_id]
        else:
            normalize_ratio = 1

        normalized_ratio = normalize_ratio * ratio * 10000
        vals_total += normalized_ratio
        data_dict[grid_id] = normalized_ratio

    ret_df = pandas.DataFrame()
    
    ids = list(data_dict.keys())
    vals = list(data_dict.values())

    ret_df = pandas.Series(vals, index=ids)
        
    return ret_df

test_datas = None

def get_data(violation_descriptions, dows, start_time, end_time, 
        start_hour, end_hour, wards, dept_categories, ticket_queues,
        include_cbd, agg_mode, chart_by):

    #hash of dict = cache file location
    md5obj = md5()
    md5obj.update(str([violation_descriptions, dows, start_time,
        end_time, start_hour, end_hour, wards, dept_categories,
        ticket_queues, include_cbd, agg_mode]).encode('utf-8'))

    request_hash = md5obj.hexdigest()

    cached_geojson = get_cached_geojson(request_hash)

    if cached_geojson:
        return cached_geojson

    new_datas = datas.copy()

    if 'All' not in violation_descriptions:
        violation_descriptions = list(map(str.upper, violation_descriptions))
        new_datas = new_datas[new_datas.violation_description.isin(violation_descriptions)]

    if 'All' not in dows:
        new_datas = new_datas[new_datas.dow.isin(dows)]
    
    if start_hour != 0:
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

    elif agg_mode == 'due': 
        pre_geojson_df = new_datas.groupby('grid_id').current_amount_due.sum()

    elif agg_mode == 'paid':
        pre_geojson_df = new_datas.groupby('grid_id').total_payments.sum()

    elif agg_mode == 'penalties':
        pre_geojson_df = new_datas.groupby('grid_id').penalty.sum()

    #if normalize_by:
    #    pre_geojson_df = normalize_geojson_data(pre_geojson_df, normalize_by)

    timeseries_ret = timeseries_from_df(new_datas, agg_mode, field_by=chart_by)
    geojson_ret = geojson_from_df(pre_geojson_df)

    if viz_config.environment == "prod":
        cache_results(geojson_ret, request_hash)

    ret = {'geojson': geojson_ret, 'timeseries_data': timeseries_ret}

    return ret

app = Flask(__name__)
api = Api(app)

def cache_results(geojson_data, filename, path=viz_config.cache_dir):
    try:
        with open('{}/{}'.format(path, filename), 'w') as fh:
            json.dump(geojson_data, fh)
    except:
        print(path, filename)
        print("oh noooo")

def get_cached_geojson(filename):
    try:
        with open('{}/{}'.format(path, viz_config.cache_dir)) as fh:
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
        #normalize_by = req_json['normalize_by'] #default = None
        chart_by = req_json['chart_by']

        ret = get_data(violation_descriptions, dows, start_time,
                       end_time, start_hour, end_hour, wards,
                       dept_categories, ticket_queues, 
                       include_cbd, agg_mode, chart_by)

        return json.dumps(ret)

api.add_resource(GeoJsonEndpoint, '/')

if __name__ == '__main__':
    if viz_config.environment in ['dev','superprod']: 
        app.run(debug=True)
    elif viz_config.environment in 'prod': 
        app.run(debug=False)
