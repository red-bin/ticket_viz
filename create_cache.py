#!/usr/bin/python3

import psycopg2 
import csv 

from sys import argv
from dropdown_opts import violation_opts

import viz_config

def pg_conn():  
     conn = psycopg2.connect(viz_config.connection_str)  
     return conn

def write_to_file():
    w = csv.writer(open('/opt/ticket_viz/data/tickets.{}.csv'.format(environment),'w'))

    cols = ['grid_id', 'violation_description', 'issue_date', 'dow', 'year','hour', 'ward', 'department_category', 'ticket_queue', 'is_business_district', 'current_amount_due', 'total_payments', 'penalty']

    w.writerow(cols)

    for row in curs.fetchall():
        row = list(row)
        row[1] = violation_opts.index(row[1])
        w.writerow(row)

conn = pg_conn() 
curs = conn.cursor() 

environment = argv[1]

sqlstr = """
SELECT grid_id, violation_description, make_date(year, month, dom) as issue_date, dow::int, year::int, hour::int, ward::int, 
    department_category, ticket_queue, is_business_district, current_amount_due, total_payments, penalty 
FROM tickets
WHERE grid_id >= 1 
AND issue_date > %s
GROUP BY grid_id, violation_description, make_date(year, month, dom), dow, year, hour, ward, department_category,
    ticket_queue, is_business_district, current_amount_due, total_payments, penalty ;"""

if environment == 'dev' or environment == "all":
    print("Creating dev cache file. 2018-03-01-")
    curs.execute(sqlstr, ['2018-03-01'])
    write_to_file()

if environment == "superprod" or environment == "all":
    print("Creating superprod cache file. 1970-2018")
    curs.execute(sqlstr, ['1970-01-01'])
    write_to_file()

if environment == "prod" or environment == "all":
    print("Creating prod cache file. 2013-2018")
    curs.execute(sqlstr, ['2013-01-01'])
    write_to_file()


