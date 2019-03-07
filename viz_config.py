#!/usr/bin/python3

import json
from os import environ

conf_fp = '{}/.viz_config'.format(environ['HOME'])
with open(conf_fp, 'r') as conf_fh:
    conf = json.load(conf_fh)

connection_str = "dbname={} host={} user={} password={}"
conn_vars = (conf['db_name'], conf['db_host'], conf['db_user'], conf['db_pass'])

connection_str = connection_str.format(*conn_vars)
cache_dir = conf['cache_dir']
dropdown_dir = conf['dropdown_dir']
census_csv = conf['census_csv']
empty_grid_geojson = conf['empty_grid_geojson']
environment = conf['environment']

if environment in ['prod', 'dev', 'superprod']:
    tickets_csv = conf['tickets_csv'].format(environment)
else:
    print('environment must be "prod" or "dev" or "superprod"')
    exit(1)

start_date = conf['date_ranges'][environment][0]
end_date = conf['date_ranges'][environment][1]

selectors =  conf['selectors']
