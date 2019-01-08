#!/usr/bin/python3

from os.path import dirname, join
from datetime import date
import requests

from datetime import datetime

import pickle

import csv
import json

import seaborn as sns

from bokeh.io import show, output_notebook, output_file, curdoc
from bokeh.models import (
    GeoJSONDataSource,
    HoverTool,
    LinearColorMapper,
    LogColorMapper,
    Div,
    DateRangeSlider,
    RangeSlider,
    Select,
    MultiSelect,
    RadioButtonGroup,
    ColorBar,
    BasicTicker,
    Toggle,
    LogTicker,
    WMTSTileSource
)
from bokeh.tile_providers import CARTODBPOSITRON as tileset
#from bokeh.tile_providers import STAMEN_TONER as tileset

from time import sleep
import geojson

from bokeh.models.tools import WheelZoomTool, PanTool

from bokeh.plotting import figure
from bokeh.palettes import Greys9 as palette
from bokeh.models.widgets import Slider, Select, TextInput
from bokeh.models.widgets import Button
from bokeh.layouts import layout, widgetbox

from dropdown_opts import violation_opts

from bokeh.transform import log_cmap, linear_cmap

violation_opts = [i.title() for i in violation_opts]
violation_opts[0] = 'All'

def get_new_data():
    weekdays = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

    def request_geojson(min_date, max_date, violation_descriptions, dows,
            min_hour, max_hour, wards, dept_categories, ticket_queues,
            include_cbd, agg_mode):

        request_json = {
            'violation_descriptions': violation_descriptions,
            'dows': dows,
            'start_time': min_date.strftime('%Y-%m-%d'),
            'end_time': max_date.strftime('%Y-%m-%d'),
            'start_hour': min_hour,
            'end_hour': max_hour,
            'wards': wards,
            'dept_categories': dept_categories,
            'ticket_queues': ticket_queues,
            'include_cbd': include_cbd,
            'agg_mode': agg_mode
        }

        data = {'data': json.dumps(request_json)}
        ret = requests.get('http://localhost:5000/', data=data)

        print(type(ret.json()))

        return ret.json()

    min_date, max_date = date_selector.value_as_datetime
    min_hour, max_hour = hours_selector.value

    if 'All' in dow_selector.value:
        dows = ['All']
    else:
        dows = [weekdays.index(i) for i in dow_selector.value]

    
    include_cbd = False if central_bus_toggle.active else True
    if select_type_radios.active == 0:
        agg_mode = 'count'
    elif select_type_radios.active == 1:
        agg_mode = 'due'
    elif select_type_radios.active == 2:
        agg_mode = 'paid'
    elif select_type_radios.active == 3:
        agg_mode = 'penalties'

    ret_geojson = request_geojson(min_date=min_date, 
                     max_date=max_date, 
                     violation_descriptions=tuple(violation_selector.value),
                     dows=tuple(dows),
                     min_hour=round(min_hour),
                     max_hour=round(max_hour),
                     wards=tuple(wards_selector.value),
                     dept_categories=tuple(dpt_categ_selector.value),
                     ticket_queues=tuple(queue_selector.value),
                     include_cbd=include_cbd,
                     agg_mode=agg_mode)

    return ret_geojson

with open('/opt/ticket_viz/cache/308022eab500a094b346e6c75c5868ee','r') as f: 
    geo_source = GeoJSONDataSource(geojson=f.read())

date_selector = DateRangeSlider(title='Date', start=date(2013,1,1), 
    value=(date(2013,1,1), date(2018,5,14)), end=date(2018,5,14), 
    step=1, callback_policy="mouseup")

hours_selector = RangeSlider(title="Hours Range", start=0, end=23, value=(0,24))

violation_selector = MultiSelect(title='Violation:', value=['All'], options=violation_opts, size=5)
violation_selector.width=100

dpt_categories = ['All', 'DOF', 'CTA', 'Speed', 'Red light', 'LAZ', 
                 'CPD', 'Chicago Parking Meter', 'Miscellaneous/Other', 
                 'Streets and San', 'SERCO']

dpt_categ_selector = MultiSelect(title='Department Category:', value=['All'], options=dpt_categories)

weekdays = ['All', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
dow_selector = MultiSelect(title='Weekdays:', value=['All'], options=weekdays)

ticket_queues = ['All', 'Hearing Req', 'Notice', 'Dismissed', 'Bankruptcy', 'Paid', 'Define', 'Court']
queue_selector = MultiSelect(title='Ticket Queue:', value=['All'], options=ticket_queues)

source_json = geojson.loads(geo_source.geojson)
data_vals = [source_json[i]['properties']['data_val'] for i in range(len(source_json['features']))]
min_val = min(data_vals)
max_val = max(data_vals)


#palette = sns.cubehelix_palette(128, start=.5, rot=-.90).as_hex()
palette = sns.cubehelix_palette(128, dark=0).as_hex()
#palette = sns.dark_palette("red").as_hex()

color_mapper = LogColorMapper(palette, low=min_val, high=max_val)

tooltips = [("Ticket Count", "@data_val")]


geomap = figure(background_fill_color=None, plot_width=600, tooltips=tooltips, tools='',  x_axis_type="mercator", y_axis_type="mercator", x_range=(-9789724.66045665, -9742970.474323519), y_range=(5107551.543942757,5164699.247119262))

geomap.title.text = "Chicago Parking Tickets Map"

geomap.add_tile(tileset)

geomap.xaxis.visible = False
geomap.yaxis.visible = False
geomap.grid.grid_line_color = None

wheel_zoom = WheelZoomTool()
pan_tool = PanTool()
tools = (wheel_zoom, pan_tool)

geomap.add_tools(*tools)
geomap.toolbar.active_scroll = wheel_zoom

geomap.patches('xs','ys', source=geo_source, fill_color={'field': 'data_val', 'transform': color_mapper}, line_color='black', line_width=.01, fill_alpha=0.9)

#with open('/opt/data/shapefiles/Boundaries - City.geojson') as f:
#    boundary_geodata = GeoJSONDataSource(geojson=f.read())
#geomap.patches('xs','ys', source=boundary_geodata, fill_color=None, line_color='#212F3D')
 
def update():
    start_time = datetime.now()
    print("Updating...")
    update_button.label = "Updating..."
    update_button.disabled = True

    source_json_str = get_new_data()
    source_json = json.loads(source_json_str)

    geo_source.geojson = source_json_str

    data_vals = [source_json['features'][i]['properties']['data_val'] for i in range(len(source_json['features']))]

    tmp_data_vals = []
    for val in data_vals:
        val = 0 if not val else val
        tmp_data_vals.append(val)

    data_vals = tmp_data_vals

    if not data_vals:
        min_val = 0
        max_val = 1

    else:
        min_val = round(min(data_vals))
        max_val = round(max(data_vals))

    if min_val < 0:
        min_val = 0
    if max_val <= 0:
        max_val = 1


    color_mapper.low = min_val
    color_mapper.high = max_val

    end_time = datetime.now()
    time_delta = (end_time - start_time).seconds

    hover_tool = [t for t in geomap.tools if t.ref['type'] == 'HoverTool'][0]
    print(hover_tool)
    
    if select_type_radios.active == 0:
        hover_tool.tooltips = [("Ticker Count", "@data_val")]

    if select_type_radios.active == 1:
        hover_tool.tooltips = [("$ Dued", "@data_val")]

    if select_type_radios.active == 2:
        hover_tool.tooltips = [("$ Paid", "@data_val")]
        
    if select_type_radios.active == 3:
        hover_tool.tooltips = [("$ in Penalties", "@data_val")]
        
    print("done updating - took {} seconds".format(time_delta))
    update_button.label = "Update"
    update_button.disabled = False

update_button = Button()
update_button.label = "Update"
update_button.on_click(update)

central_bus_toggle = Toggle(label="Ignore Central Business District", active=False)

rbg_div = Div(text="Display by: ")
select_type_radios = RadioButtonGroup(
        labels=["Count", "Due", "Paid", "Penalties"], active=0)

wards_opts = ['All'] + list(map(str, range(1,51)))
wards_selector = MultiSelect(title="Ward #:", value=['All'], options=wards_opts, size=5)

#ticker
color_bar = ColorBar(color_mapper=color_mapper, ticker=LogTicker(desired_num_ticks=8), location=(0,0))
geomap.add_layout(color_bar, 'right')
geomap.right[0].formatter.use_scientific = False

controls = [date_selector, hours_selector, dow_selector, violation_selector, wards_selector, 
            dpt_categ_selector, queue_selector, rbg_div, select_type_radios, central_bus_toggle,  update_button]

inputs = widgetbox(*controls, sizing_mode='fixed', width=400)

l = layout([[inputs, geomap]], sizing_mode='fixed')

curdoc().add_root(l)
