#/usr/bin/python3

from os import listdir

import folium
import moviepy.editor as mpy

import re

import datetime
import psycopg2
import geojson
from geopy.distance import geodesic
from multiprocessing import Pool

from selenium import webdriver
from time import sleep


from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options


def pg_conn():
    connstr = "dbname=tickets host=localhost user=loolllol password=loaldsof"
    conn = psycopg2.connect(connstr)

    return conn

def save_geojson(paths, savepath):
    palette = {
        'DOF':'#1f78b4',
        'CTA':'#33a02c',
        'Speed':'#6a3d9a',
        'Red light':'#a6cee3',
        'Chicago Parking Meter':'#b2df8a',
        'Miscellaneous/Other':'#cab2d6',
        'Streets and San':'#e31a1c',
        'LAZ':'#fb9a99',
        'CPD':'#fdbf6f',
        'SERCO':'#ff7f00'
    }

    features = []
    for officer, geoms in paths.items():
        officer_dept = officer.split('|')[1]
        color = palette[officer_dept]
        if not geoms:
            continue

        for geom in geoms:
            ls = geojson.LineString(geom)
            feature = geojson.Feature(geometry=ls, properties={'stroke':color, 'stroke-width':1})
            features.append(feature)

    fh = open(savepath, 'w')
    results = geojson.FeatureCollection(features)
    geojson.dump(results, fh)
    fh.close()

    return results

times = []

chunk_len = 1
chunk_count = 24

#td = datetime.timedelta(hours=chunk_len)

dt = datetime.datetime(2012, 6, 1, 0, 0, 0)

#times = [ (datetime.datetime(2012, i, 1, 0, 0, 0).strftime('%Y-%m-%d %H:%M'), datetime.datetime(2012, i+1, 1, 0, 0, 0).strftime('%Y-%m-%d %H:%M')) for i in range(1,12)]

#times = [((dt+(td*i)).strftime('%Y-%m-%d %H:%M'), (dt+(td*(i+1))).strftime('%Y-%m-%d %H:%M')) for i in range(0,chunk_count)]

times = [i for i in range(0,24) ]
def get_tickets(time):
    conn = pg_conn()
    curs = conn.cursor()
    #start, end = inputs
    sqlstr = """
        SELECT 
          concat(t.officer, '|', u.department_category), 
          t.geocoded_lng, t.geocoded_lat, issue_date
        FROM tickets t, units u
        WHERE issue_date >= '2012-01-01'
        AND issue_date < '2013-01-01'
        AND extract('hour' from issue_date) = {}
        AND u.unit = t.unit
        ORDER BY t.officer, issue_date asc""".format(time)

    #curs.execute(sqlstr, (start, end))
    curs.execute(sqlstr)
    tickets = curs.fetchall()

    return (time, tickets)

pool = Pool(processes=24)
chunks = pool.map(get_tickets, times)

buffer_size = 2
start_idx = 0

idxs = []

for end_idx in range(1, chunk_count):
    if end_idx < buffer_size:
        start_idx = 0
    else:
        start_idx += 1

    idxs.append((start_idx, end_idx))

def create_frame(start_idx, end_idx):
    frame_tickets = [] 

    for time, chunk in chunks[start_idx:end_idx]:
        frame_tickets += chunk

    print(len(frame_tickets))

    officers = {}

    for officer, lng, lat, dt in frame_tickets:
        if officer not in officers:
            officers[officer] = []
    
        officers[officer].append((lng, lat, dt))
    
    officer_paths = dict([(o,[]) for o in officers.keys()])
    
    for officer, vals in officers.items():
        if not vals:
            continue
    
        first_vals = vals.pop(0)
        last_lng, last_lat, last_dt = first_vals
    
        current_path = [(last_lng, last_lat)]
    
        for lng, lat, dt in vals:
            if not lat or not lng:
                continue

            distance_delta = geodesic((lng, lat), (last_lng, last_lat))
            distance_delta = distance_delta.meters
    
            dt_diff = dt - last_dt
            dt_diff = abs(dt_diff.total_seconds())
    
            if distance_delta == 0.0 or dt_diff == 0:
                last_lng = lng
                last_lat = lat
                last_dt = dt
    
                continue
    
            walking_speed = distance_delta / dt_diff
            if walking_speed < 3.0 and dt_diff < 1800 and distance_delta < 300:
                if current_path and (lng, lat) != current_path[-1]:
                    current_path.append((last_lng, last_lat))
    
            else:
                if len(current_path) > 1:
                    officer_paths[officer].append(current_path)
    
                current_path = [(lng, lat)]
    #            continue #keep old last_vars

            last_lng = lng
            last_lat = lat
            last_dt = dt
    
        if len(current_path) > 1:
            officer_paths[officer].append(current_path)

    fp = '/tmp/geojsons/{}_{}.geojson'.format(start_idx, end_idx)
    results = save_geojson(officer_paths, fp)

pool = Pool(processes=24)
pool.starmap(create_frame, idxs)

options = webdriver.ChromeOptions()
options.add_argument('window-size=678,1060')

driver = webdriver.Chrome(chrome_options=options)


def html_to_png(page_fp):
    print(page_fp)
#    firefox_options = Options()
    #firefox_options.add_argument()

    page_path = "file://%s" % page_fp
    driver.get(page_path)
    driver.execute_script('document.getElementById("{}").style.background="#000";'.format(driver.find_element_by_class_name('folium-map').get_property('id')))

    #delete zoom
    try:
        zoom_elem = driver.find_element_by_xpath("/html/body/div/div[2]/div[1]")
        driver.execute_script("""var element = arguments[0];
                            element.parentNode.removeChild(element);""", zoom_elem)
    except:
        print("unable to remove zoom...")

    #save map elem
    filename = re.split('[/.]', page_fp)[-2]
    screenshot_savepath = "/opt/data/tickets/cache/screenshots/%s.png" % filename
    #map_elem = driver.find_element_by_tag_name("div")
    #map_elem.screenshot(screenshot_savepath)
    driver.save_screenshot(screenshot_savepath)

    return screenshot_savepath

pngs = []


for fn in listdir('/tmp/geojsons/'):
    fp = '/tmp/geojsons/{}'.format(fn)
    save_fn = '{}.html'.format(fn.split('.')[0])

    fh = open(fp, 'r')
    source_data = fh.read()

    m1 = folium.Map(tiles=None, location=(41.8481, -87.701), zoom_start=11.4,
                    height=930, width=670)

    style_function = lambda x: {
        'color' : x['properties']['stroke'],
        'weight' : x['properties']['stroke-width']
    }

    folium.GeoJson(source_data, style_function=style_function).add_to(m1)

    save_fp = '/tmp/htmls/{}'.format(save_fn)
    m1.save(save_fp)

    print(save_fp)
    pngs.append(html_to_png(save_fp))

png_fps = []
for start, end in idxs:
    png_fp = '/opt/data/tickets/cache/screenshots/{}_{}.png'.format(start, end)
    png_fps.append(png_fp)


clip = mpy.ImageSequenceClip(png_fps, fps=1, load_images=True)
clip.write_videofile('/opt/data/test.webm')
