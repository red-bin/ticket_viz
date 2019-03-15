#!/usr/bin/python3

import psycopg2cffi
import csv 
import time
from datetime import date
from datetime import datetime 

import viz_config as conf

def pg_conn():  
     conn = psycopg2cffi.connect(conf.connection_str)  
     return conn

conn = pg_conn() 
curs = conn.cursor() 

def ticket_data():
    sqlstr = """
    SELECT grid_id, violation_description, make_date(year, month, dom) as issue_date, dow::int, year::int, hour::int, ward::int, 
        department_category, ticket_queue, is_business_district, current_amount_due::numeric::int, total_payments::numeric::int, penalty::numeric::int, fine_level1_amount::numeric::int, hearing_disposition
    FROM tickets
    WHERE grid_id >= 1 
    AND issue_date >= %s
    AND issue_date <= %s
    GROUP BY grid_id, violation_description, make_date(year, month, dom), dow, year, hour, ward, department_category, ticket_queue, is_business_district, current_amount_due, total_payments, penalty, fine_level1_amount, hearing_disposition
    ORDER BY issue_date
    """
    
    print_vars = (conf.environment, conf.start_date, conf.end_date)
    print("Creating {} cache file. {} - {}".format(*print_vars))
    curs.execute(sqlstr, (conf.start_date, conf.end_date))
    
    results = curs.fetchall()

    return results

def selector_opts(field_name, sorted_by, order='desc', cache=True):
    """sorted_by can be 'field_count', 'field'
       order must be 'asc' or 'desc'"""

    if sorted_by == 'field_count':
        sqlstr = """
            SELECT {}, count({})
            FROM tickets
            WHERE issue_date >= '{}'
            AND issue_date <= '{}'
            GROUP BY {}
            ORDER BY count({}) {}
        """
    elif sorted_by == 'field':
        sqlstr = """
            SELECT {}, count({})
            FROM tickets
            WHERE issue_date >= '{}'
            AND issue_date <= '{}'
            GROUP BY {}
            ORDER BY {} {}
        """
 
    print("Creating {} opts".format(field_name))
    sql_vars = (field_name, field_name, conf.start_date, conf.end_date, field_name, field_name, order)
    curs.execute(sqlstr.format(*sql_vars))
    
    results = curs.fetchall()
    total = sum([i[1] for i in results])

    dropdown_vals = ['{} ({})'.format(i[0],i[1]) for i in results if i[1]]
    dropdown_vals.insert(0, 'All ({})'.format(total))

    return dropdown_vals

def create_cache():
    opts_txt = {}

    for selector in conf.selectors:
        column_name = selector['column_name']
        sorted_by = selector['sorted_by']
        order = selector['order']

        opts = selector_opts(column_name, sorted_by, order)

        #cache_opts
        fp = '{}/{}.{}.txt'.format(conf.dropdown_dir, column_name, conf.environment)
        with open(fp, 'w') as fh:
            fh.write('\n'.join(opts))
 
        opts_txt[column_name] = []
        for opt in opts:
            opts_txt[column_name].append(' '.join(opt.split(' ')[:-1]))

    weeks_start = date(1995,12,31) #1 day before min date in data: has dow of 0.
    date_format = '%Y-%m-%d'

    w = csv.writer(open('/opt/ticket_viz/data/tickets.{}.csv'.format(conf.environment),'w'))
    cols = ['grid_id', 'violation_description', 'dow', 'year','hour', 'ward', 'department_category', 'ticket_queue', 'is_business_district', 'current_amount_due', 'total_payments', 'penalty', 'fine_level1_amount', 'hearing_disposition', 'week_idx', 'day_idx', 'month_idx']
    w.writerow(cols)

    indexable_cols = [(s['column_name'], cols.index(s['column_name'])) for s in conf.selectors]

    data = ticket_data()
    for row in data:
        dow = row[3]
        issue_date = row[2]

        delta = issue_date - weeks_start 
        day_idx = delta.days
        week_idx = int(day_idx / 7)

        if issue_date.month < 10:
            month_idx = '{}0{}'.format(issue_date.year, issue_date.month)
        else:
            month_idx = '{}{}'.format(issue_date.year, issue_date.month)

        new_row = list(row)
        new_row.pop(2) #only temporarily needed

        for column_name, col_idx in indexable_cols:
            col_val = str(new_row[col_idx])
            new_row[col_idx] = opts_txt[column_name].index(col_val)

        w.writerow(new_row + [week_idx, day_idx-1, month_idx])

print("=== Creating cached data for {} ===".format(conf.environment))
create_cache()
