#!/usr/bin/python3

import psycopg2 
import csv 

def pg_conn():  
     connstr = "dbname=tickets host=localhost user=sdjfklsjglsg password=sdjfksjga"  
     conn = psycopg2.connect(connstr)  
     return conn  
 
conn = pg_conn() 
curs = conn.cursor() 
 
curs.execute("""
SELECT grid_id, violation_description, make_date(year, month, dom) as issue_date, dow::int, year::int, hour::int, ward::int, 
    department_category, ticket_queue, is_business_district, current_amount_due, total_payments, penalty 
FROM tickets
WHERE grid_id >= 1 
AND issue_date > '2013-01-01'
GROUP BY grid_id, violation_description, make_date(year, month, dom), dow, year, hour, ward, department_category,
    ticket_queue, is_business_district, current_amount_due, total_payments, penalty ;""")
 
w = csv.writer(open('/dev/shm/tickets.csv','w'))

cols = ['grid_id', 'violation_description', 'issue_date', 'dow', 'year','hour', 'ward', 'department_category', 'ticket_queue', 'is_business_district', 'current_amount_due', 'total_payments', 'penalty']

w.writerow(cols)
w.writerows(curs.fetchall())
