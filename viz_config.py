#!/usr/bin/python3

import json
from os import environ

conf_fp = '{}/.viz_config'.format(environ['HOME'])
with open(conf_fp, 'r') as conf_fh:
    conf = json.load(conf_fh)

connection_str = "dbname={} host={} user={} password={}"
conn_vars = (conf['db_name'], conf['db_host'], conf['db_user'], conf['db_pass'])

connection_str = connection_str.format(*conn_vars)
debug_mode = conf['debug_mode']
cache_dir = conf['cache_dir']
census_geojson = conf['census_geojson']
