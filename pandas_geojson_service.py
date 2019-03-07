#!/usr/bin/python3

import csv
import json
import pandas as pd

from hashlib import md5

from datetime import datetime, timedelta

from os import stat

from flask import Flask, request
from flask_restful  import Resource, Api

import viz_config as conf

column_types = {
    'grid_id':'uint16',
    'violation_description':'uint8',
    'dow':'uint16',
    'year':'uint16',
    'hour':'uint8',
    'ward':'uint8',
    'department_category':'uint8',
    'ticket_queue':'uint8',
    'is_business_district':bool,
    'current_amount_due':'int16',
    'total_payments':'int16',
    'penalty':'int16',
    'fine_level1_amount': 'int16',
    'hearing_disoposition':'uint8',
    'week_idx': 'uint16',
    'day_idx': 'uint16',
    'month_idx': 'uint32',
}


print('Loading ', conf.tickets_csv)
datas = pd.read_csv(conf.tickets_csv, dtype=column_types)

max_week_idx = max(datas.week_idx)
max_day_idx = max(datas.day_idx)

print(max_week_idx, max_day_idx)

date_format = '%Y-%m-%d'
data_beginning = datetime.strptime('1995-12-31', date_format)

cached_week_strs = {}
cached_day_strs = {}

for week_idx in range(0, max_week_idx+1):
    time_dt = data_beginning + timedelta(days=week_idx*7)
    time_str =  time_dt.strftime(date_format)
    cached_week_strs[week_idx] = time_str

for day_idx in range(0, max_day_idx+1):
    time_dt = data_beginning + timedelta(days=day_idx)
    time_str = time_dt.strftime(date_format)
    cached_day_strs[day_idx] = time_str

cached_opts = {}
for selector in conf.selectors:
    name = selector['column_name']

    with open('{}/{}.{}.txt'.format(conf.dropdown_dir, name, conf.environment), 'r') as fh:
        cached_opts[name] = [i.rstrip() for i in fh.readlines()]

with open(conf.empty_grid_geojson, 'r') as fh:
    empty_grid_json = json.load(fh)

def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

print(mem_usage(datas))

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

def timeseries_from_df(df, agg_mode, aggreg_by, chart_mode, resolution_mode):
    print('timeseries info:', agg_mode, aggreg_by, chart_mode, resolution_mode)
    key_data = {}
    cumm_dict = {}


    for time, chunk_df in df.groupby(resolution_mode): 
        if agg_mode == 'count':
            chunk_data = chunk_df.groupby(aggreg_by).is_business_district.size()
        elif agg_mode == 'due':
            chunk_data = chunk_df.groupby(aggreg_by).current_amount_due.sum()
        elif agg_mode == 'paid':
            chunk_data = chunk_df.groupby(aggreg_by).total_payments.sum()
        elif agg_mode == 'penalties':
            chunk_data = chunk_df.groupby(aggreg_by).penalty.sum()
        elif agg_mode == 'fine_level1_amount':
            chunk_data = chunk_df.groupby(aggreg_by).penalty.sum()

        for key, val in dict(chunk_data).items():
            if key not in key_data.keys():
                key_data[key] = {'xs':[], 'ys':[]}

            if chart_mode == 'cummulative' and key_data[key]['xs']:
                val = val + key_data[key]['ys'][-1]

            if resolution_mode == 'week_idx':
                time_str = cached_week_strs[time]

            elif resolution_mode == 'day_idx':
                time_str = cached_day_strs[time]

            elif resolution_mode == 'year':
                time_str = '{}-01-01'.format(time)

            elif resolution_mode == 'month_idx':
                month = str(time)[-2:]
                year = str(time)[:4]
                time_str = '{}-{}-01'.format(year, month)

            key_data[key]['xs'].append(time_str) ##FIX THIS
            key_data[key]['ys'].append(int(val))

    ret_dict = {'keys':[], 'xs':[], 'ys':[]}
    for key, vals in key_data.items():
        ret_dict['keys'].append(key)
        ret_dict['xs'].append(vals['xs'])
        ret_dict['ys'].append(vals['ys'])

    new_keys = []
    for k in ret_dict['keys']:
        new_keys.append(cached_opts[aggreg_by][k])
    ret_dict['keys'] = new_keys

    return ret_dict

def get_data(agg_mode='count', start_time=None, end_time=None, aggreg_by=None, chart_mode=None, resolution_mode=None, **kwargs):
    #hash of dict = cache file location
    md5obj = md5()

    md5_kwargs = [kwargs[k] for k in sorted(kwargs.keys())]

    md5_input = '|'.join(map(str, (start_time, end_time, agg_mode, aggreg_by, chart_mode, resolution_mode, md5_kwargs)))

    md5obj.update(md5_input.encode('utf-8'))

    request_hash = md5obj.hexdigest()

    cached_geojson = get_cached_geojson(request_hash)
    

    if cached_geojson:
        print("Returning cached version")
        return cached_geojson

    new_datas = datas.copy()

    #filter selection based on selector inputs. 0 = 'All' = skip
    for selector_name, selector_vals in kwargs['selector_vals']:
        if 0 in selector_vals:
            continue

        new_datas = new_datas[new_datas[selector_name].isin(selector_vals)]

    if not kwargs['include_cbd']:
        new_datas = new_datas[new_datas.is_business_district != True]
    if kwargs['start_hour'] != 0:
        new_datas = new_datas[new_datas.hour >= start_hour]
    if kwargs['end_hour'] != 24:
        new_datas = new_datas[new_datas.hour <= end_hour]
  
    date_format = '%Y-%m-%d'
    data_beginning = datetime.strptime('1995-12-31', date_format) 

    day_start_idx = (start_time - data_beginning).days
    day_end_idx = (end_time - data_beginning).days

    week_start_idx = int(day_start_idx / 7)
    week_end_idx = int(day_end_idx / 7)

    conf_start_date = datetime.strptime(conf.start_date, '%Y-%m-%d')
    conf_end_date = datetime.strptime(conf.end_date, '%Y-%m-%d')

    if start_time.year > conf_start_date.year:
        new_datas = new_datas[new_datas.year >= start_time.year]

    if end_time.year < conf_end_date.year:
        new_datas = new_datas[new_datas.year <= start_time.year]

    if start_time > conf_start_date:
        new_datas = new_datas[new_datas.day_idx >= day_start_idx]
        new_datas = new_datas[new_datas.week_idx >= week_start_idx]

    if end_time < conf_end_date:
        new_datas = new_datas[new_datas.day_idx <= day_end_idx]
        new_datas = new_datas[new_datas.week_idx <= week_end_idx]

    #agg_mode check must come last
    if agg_mode == 'count': 
        pre_geojson_df = new_datas.groupby('grid_id').size()

    elif agg_mode == 'due': 
        pre_geojson_df = new_datas.groupby('grid_id').current_amount_due.sum()

    elif agg_mode == 'paid':
        pre_geojson_df = new_datas.groupby('grid_id').total_payments.sum()

    elif agg_mode == 'penalties':
        pre_geojson_df = new_datas.groupby('grid_id').penalty.sum()

    elif agg_mode == 'fine_level1_amount':
        pre_geojson_df = new_datas.groupby('grid_id').fine_level1_amount.sum()

    timeseries_ret = timeseries_from_df(new_datas, agg_mode, aggreg_by=aggreg_by, chart_mode=chart_mode, resolution_mode=resolution_mode)
    geojson_ret = geojson_from_df(pre_geojson_df)

    print('ts_len: ', len(timeseries_ret))

    ret = {'geojson': geojson_ret, 'timeseries_data': timeseries_ret}

    if conf.environment in ['prod', 'superprod']:
        cache_results(ret, request_hash)

    return ret

app = Flask(__name__)
api = Api(app)

def cache_results(geojson_data, filename, path=conf.cache_dir):
    try:
        with open('{}/{}'.format(path, filename), 'w') as fh:
            json.dump(geojson_data, fh)
    except:
        print(path, filename)
        print("oh noooo")

def get_cached_geojson(filename):
    try:
        with open('{}/{}'.format(conf.cache_dir, filename)) as fh:
            return json.load(fh)
    except:
        print("Failed to get cache!")
        return None

class GeoJsonEndpoint(Resource):
    def get(self):
        print(request.form['data'])

        req_json = json.loads(request.form['data'])
        req_json['end_time'] = datetime.strptime(req_json['end_time'], '%Y-%m-%d')
        req_json['start_time'] = datetime.strptime(req_json['start_time'], '%Y-%m-%d')

        ret = get_data(**req_json)

        return json.dumps(ret)

api.add_resource(GeoJsonEndpoint, '/')

if __name__ == '__main__':
    if conf.environment in ['dev']: 
        app.run(debug=True)
    elif conf.environment in ['prod', 'superprod']: 
        app.run(debug=False)
