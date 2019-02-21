#!/usr/bin/python3

from os.path import dirname, join
from datetime import date
import requests

from datetime import datetime
from datetime import timedelta

from math import pi

import csv
import json

import seaborn as sns

from bokeh.models.annotations import Title

from bokeh.io import show, output_notebook, output_file, curdoc
from bokeh.models import (
    FactorRange,
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
    WMTSTileSource,
    CategoricalScale,
    ColumnDataSource,
    CustomJS
)
from bokeh.tile_providers import CARTODBPOSITRON as tileset
#from bokeh.tile_providers import STAMEN_TONER as tileset

from time import sleep
import geojson

from bokeh.models.tools import WheelZoomTool, PanTool, ResetTool

from bokeh.plotting import figure
from bokeh.palettes import Category20
from bokeh.models.widgets import Slider, Select, TextInput
from bokeh.models.widgets import Button
from bokeh.layouts import layout, widgetbox, column

from dropdown_opts import violation_opts

from bokeh.transform import log_cmap, linear_cmap

violation_opts = [i.title() for i in violation_opts]

def get_new_data():
    weekdays = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    def request_data(min_date, max_date, violation_descriptions, dows,
            min_hour, max_hour, wards, dept_categories, ticket_queues,
            include_cbd, agg_mode, chart_by, chart_mode):

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
            'agg_mode': agg_mode,
            #'normalize_by': normalize_by,
            'chart_by': chart_by,
            'chart_mode': chart_mode,
        }

        data = {'data': json.dumps(request_json)}
        ret = requests.get('http://localhost:5000/', data=data)

        return ret.json()

    min_date, max_date = date_selector.value_as_datetime
    min_hour, max_hour = hours_selector.value

    if 'All' in dow_selector.value:
        dows = ['All']
    else:
        dows = [weekdays.index(i) for i in dow_selector.value]

    include_cbd = False if central_bus_toggle.active else True

    agg_modes = ['count', 'due', 'paid', 'penalties']
    agg_mode = agg_modes[select_type_radios.active]

    #if select_normalize_radios.active == 0:
    #    normalize_by = None
    #elif select_normalize_radios.active == 1:
    #    normalize_by = 'total_population'
    #elif select_normalize_radios.active == 2:
    #    normalize_by = 'percent'

    chart_bys = ['violation_description', 'department_category', 'ward']
    chart_by = chart_bys[select_chart_radios.active]

    chart_modes = ['count', 'cummulative', 'histogram']
    chart_mode = chart_modes[chart_rbg_radios.active]

    violation_descriptions = []
    for viol in tuple(violation_selector.value):
        if viol == 'All': #remove from 
            violation_descriptions.append('All')
        else:
            violation_descriptions.append(violation_opts.index(viol))

    resp_str = request_data(min_date=min_date, 
                     max_date=max_date, 
                     violation_descriptions=violation_descriptions,
                     dows=tuple(dows),
                     min_hour=round(min_hour),
                     max_hour=round(max_hour),
                     wards=tuple(wards_selector.value),
                     dept_categories=tuple(dpt_categ_selector.value),
                     ticket_queues=tuple(queue_selector.value),
                     include_cbd=include_cbd,
                     agg_mode=agg_mode,
                     #normalize_by=normalize_by,
                     chart_by=chart_by,
                     chart_mode=chart_mode)

    ret = json.loads(resp_str)
    return ret['geojson'], ret['timeseries_data']

def clear_chart():
    global chart_lines
    if chart_lines:
        chart.renderers.remove(chart_lines)

def update_chart(timeseries_data):
    #chart_palette = sns.color_palette("Paired", len(timeseries_data['xs'])).as_hex()
    ts_size = len(timeseries_data['xs'])
    chart_palette = sns.hls_palette(ts_size, l=.5, s=.6).as_hex()

    chart_modes = ['count', 'cummulative', 'histogram']
    chart_mode = chart_modes[chart_rbg_radios.active]

    clear_chart()

    #if time-based, convert x axis to datetime
    if chart_mode in ['count', 'cummulative']:
        #chart.x_scale = None

        datetime_xs = []
        for raw_xs in timeseries_data['xs']:
            datetime_xs.append([datetime.strptime(x, '%Y-%m-%d') for x in raw_xs])
    
        timeseries_data['xs_datetime'] = datetime_xs

        timeseries_data['line_color'] = chart_palette
    
    
    #    for key, vals in timeseries_data.items():
        line_opts = {
            'xs': 'xs_datetime',
            'ys': 'ys',
            'line_width': 1.5,
            'line_alpha': .7,
            'line_color': 'line_color',
            'source': ColumnDataSource(timeseries_data),
        }

        min_date, max_date = date_selector.value_as_datetime
        inbetween = [min_date + timedelta(days=x) for x in range(0, (max_date - min_date).days)]
        inbetween_str = [datetime.strftime(d, '%Y-%m-%d') for d in inbetween] 

        #del chart.x_range.factors 
        global chart_lines
        #chart_lines = chart.multi_line(**line_opts)
        chart_lines = chart.multi_line(**line_opts)
    
        chart_tooltips=[
            ("value", "$y{int}"),
            ("Date", "$x{%F}"),
            ("name", "@keys"),
        ]

        chart_bys = ['Violation Name', 'Department Class', 'Ward no.']
        chart_by = chart_bys[select_chart_radios.active]
 
        chart_modes = ['Daily Count', 'Cummulative']
        chart_mode = chart_modes[chart_rbg_radios.active]

        chart_title.text = '{} of tickets by {}'.format(chart_mode, chart_by)

        for tool in chart.tools:
            if tool.name == 'chart_hovertool':
                del tool

        chart_hovertool = HoverTool(tooltips=chart_tooltips, formatters={'$x': 'datetime'}, line_policy='nearest', mode='mouse', name='chart_hovertool')
        chart.add_tools(chart_hovertool)
        chart.xaxis.visible = True


def update():
    start_time = datetime.now()
    print("Updating...")
    update_button.label = "Updating..."
    update_button.disabled = True

    geojson_data, timeseries_data = get_new_data()

    update_chart(timeseries_data)

    ##update map
    geo_source.geojson = json.dumps(geojson_data)

    ##update color range
    data_vals = [geojson_data['features'][i]['properties']['data_val'] for i in range(len(geojson_data['features']))]

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

    #update hover tool dialogues
    hover_tool = [t for t in geomap.tools if t.ref['type'] == 'HoverTool'][0]
    
    if select_type_radios.active == 0:
        hover_tool.tooltips = [("Ticket Count", "@data_val")]

    if select_type_radios.active == 1:
        hover_tool.tooltips = [("$ Due", "@data_val")]

    if select_type_radios.active == 2:
        hover_tool.tooltips = [("$ Paid", "@data_val")]
        
    if select_type_radios.active == 3:
        hover_tool.tooltips = [("$ in Penalties", "@data_val")]
        
    print("done updating - took {} seconds".format(time_delta))
    update_button.label = "Update"
    update_button.disabled = False

with open('/opt/ticket_viz/data/default_geojson','r') as f: 
    geo_source = GeoJSONDataSource(geojson=f.read())

date_selector = DateRangeSlider(title='Date', start=date(2013,1,1), 
    value=(date(2013,1,1), date(2018,5,14)), end=date(2018,5,14), 
    step=1, callback_policy="mouseup")

hours_selector = RangeSlider(title="Hours Range", start=0, end=23, value=(0,24))

violation_selector = MultiSelect(title='Violation:', value=['All'], options=violation_opts, size=4)
#violation_selector.width=100

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

palette = sns.cubehelix_palette(128, dark=0).as_hex()
color_mapper = LogColorMapper(palette, low=min_val, high=max_val)

geomap_tooltips = [("Ticket Count", "@data_val")]

##create map
geomap_opts = {
    'background_fill_color': None, 
    'plot_width': 800,
    #'plot_width': 600,
    'tooltips': geomap_tooltips, 
    'tools': '',
    'x_axis_type': "mercator",
    'y_axis_type': "mercator",
    'x_range': (-9789724.66045665, -9742970.474323519),
    'y_range': (5107551.543942757,5164699.247119262),
    'output_backend': 'webgl'
}
geomap = figure(**geomap_opts)

geomap.title.text = "Chicago Parking Tickets Map"

geomap.add_tile(tileset)

geomap.xaxis.visible = False
geomap.yaxis.visible = False
geomap.grid.grid_line_color = None

##setup tools and setup wheel zoom as active
geomap_wheel_zoom = WheelZoomTool()
geomap_pan_tool = PanTool()

geomap.add_tools(geomap_wheel_zoom, geomap_pan_tool)
geomap.toolbar.active_scroll = geomap_wheel_zoom

geomap.toolbar.logo = None
geomap.toolbar_location = None

#create patches initially with empty grid
patches_opts = {
    'xs': 'xs',
    'ys': 'ys',
    'source': geo_source,
    'fill_color': {'field': 'data_val', 'transform': color_mapper}, 
    'line_color': 'black',
    'line_width': .01,
    'fill_alpha': 0.9
}
geomap.patches(**patches_opts)

##create chart

#start = datetime.datetime.strptime("21-06-2014", "%d-%m-%Y") 
#end = datetime.datetime.strptime("07-07-2014", "%d-%m-%Y")

chart_opts = {
    'plot_width':800,
    'plot_height':300,
    'tools':'',
    'x_axis_type': 'datetime',
    'output_backend': 'webgl',
    'title': '',
    #'y_axis_type': "log"
}

chart = figure(**chart_opts)
chart_title = Title()
chart_title.text = "starting title"
chart.title = chart_title
#chart_wheel_zoom = WheelZoomTool()
#chart_pan_tool = PanTool()
#chart_reset_tool = ResetTool()

#chart.add_tools(chart_reset_tool)
#chart.toolbar.active_scroll = chart_wheel_zoom

chart.xaxis.visible = False
#chart.xaxis.major_label_orientation = pi/4
chart_lines = None
chart_hover = None

 
chart.toolbar.logo = None
chart.toolbar_location = None

##create widgets
update_button = Button()
update_button.label = "Update"
update_button.on_click(update)


central_bus_toggle = Toggle(label="Ignore Central Business District", active=False)

select_rbg_div = Div(text="Display by: ")
select_type_radios = RadioButtonGroup(
        labels=["Count", "Due", "Paid", "Penalties"], active=0)

chart_rbg_div = Div(text="Chart type: ")
chart_rbg_labels = ["Per Day", "Cummulative"]
chart_rbg_radios = RadioButtonGroup(labels=chart_rbg_labels, active=0)

#implement in the future
#normalize_rbg_div = Div(text="Normalize By: ")
#select_normalize_radios = RadioButtonGroup(
#        labels=["None", "Population", "Percent"], active=0)

chart_rbg_div = Div(text="Chart Lines By: ")
select_chart_radios = RadioButtonGroup(
        labels=["Violation", "Department", "Ward"], active=0)

wards_opts = ['All'] + list(map(str, range(1,51)))
wards_selector = MultiSelect(title="Ward #:", value=['All'], options=wards_opts, size=4)

ticker = LogTicker(desired_num_ticks=8)

color_bar = ColorBar(color_mapper=color_mapper, ticker=ticker, location=(0,0))
geomap.add_layout(color_bar, 'right')
geomap.right[0].formatter.use_scientific = False

controls = [date_selector, hours_selector, dow_selector, violation_selector, wards_selector,
            dpt_categ_selector, queue_selector, select_rbg_div, select_type_radios, 
            central_bus_toggle, chart_rbg_div, 
            select_chart_radios, chart_rbg_radios, update_button]

inputs = widgetbox(*controls, sizing_mode='fixed', width=400)

col2 = column(geomap, chart)
col1 = column(inputs)

l = layout([[col1, col2]], sizing_mode='fixed')

curdoc().add_root(l)
