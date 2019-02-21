#!/usr/bin/python3

import csv
import json
import viz_config

with open(viz_config.census_geojson) as census_fh:
    census_geo_data = json.load(census_fh)

keys = [
    "TOTAL_POPULATION", "WHITE", "BLACK_OR_AFRICAN_AMERICAN", 
    "AMERICAN_INDIAN_AND_ALASKA_NATIVE", "ASIAN", 
    "NATIVE_HAWAIIAN_AND_OTHER_PACIFIC_ISLANDER", "SOME_OTHER_RACE", 
    "TWO_OR_MORE_RACES", "HISPANIC_OR_LATINO_ORIGIN"
]

keys = ["TOTAL_POPULATION"]

totals_dict = dict([(k,0) for k in keys])

census_grid_dict = {}
for feat in census_geo_data['features']:
    props = feat['properties']
    census_grid_dict[props['ID']] = props

    for key in keys:
        totals_dict[key] += int(props[key])


for grid_id, props in census_grid_dict.items():
    for key in keys:
        key_val = props[key]
        key_ratio = key_val / totals_dict[key]

        ratio_key_name = "{}_RATIO".format(key)

        props[ratio_key_name] = key_ratio
        census_grid_dict[grid_id] = props


with open('/opt/ticket_viz/data/census_grid_data.csv', 'w') as fh:
    w = csv.DictWriter(fh, props.keys())
    w.writeheader()
    w.writerows(census_grid_dict.values())
