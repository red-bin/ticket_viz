#!/usr/bin/python3

from datetime import datetime, date
import geojson
import json
import requests
import seaborn as sns

from bokeh.models.annotations import Title
from bokeh.io import curdoc
from bokeh.models import (
    GeoJSONDataSource,
    HoverTool,
    LogColorMapper,
    Div,
    DateRangeSlider,
    RangeSlider,
    MultiSelect,
    RadioButtonGroup,
    ColorBar,
    Toggle,
    LogTicker,
    ColumnDataSource,
)

from bokeh.layouts import layout, widgetbox, column
from bokeh.models.tools import WheelZoomTool, PanTool
from bokeh.models.widgets import Button
from bokeh.plotting import figure
from bokeh.tile_providers import CARTODBPOSITRON as tileset

import viz_config as conf

opts_fields = [
    'violation_description',
    'ward',
    'ticket_queue',
    'department_category',
    'dow',
    'hearing_disposition'
]

#pulling results from create_cache.py
selector_opts = {}
for opt_field in opts_fields:
    fp = '{}/{}.{}.txt'.format(conf.dropdown_dir, opt_field, conf.environment)
    print(fp)
    with open(fp, 'r') as fh:
        selector_opts[opt_field] = [i.rstrip() for i in fh.readlines()]

def selector_value(name):
    for selector in selectors:
        if selector.name == name:
            return selector.value

    print("No selector by that name!")
    return None

def controls_vals():
    min_date = date_selector.value_as_datetime[0]
    max_date = date_selector.value_as_datetime[1]
    min_hour = hours_selector.value[0]
    max_hour = hours_selector.value[1]

    include_cbd = False if central_bus_toggle.active else True

    agg_modes = ['count', 'due', 'paid', 'penalties', 'fine_level1_amount']
    agg_mode = agg_modes[select_type_radios.active]

    aggregs_by = [s['column_name'] for s in conf.selectors]
    aggreg_by = aggregs_by[select_chart_radios.active]

    chart_modes = ['count', 'cummulative', 'histogram']
    chart_mode = chart_modes[chart_rbg_radios.active]

    resolution_modes = ['year', 'month_idx', 'week_idx', 'day_idx']
    resolution_mode = resolution_modes[chart_res_radios.active]

    ret_vals = {
        'start_time': min_date.strftime('%Y-%m-%d'),
        'end_time': max_date.strftime('%Y-%m-%d'),
        'start_hour': min_hour,
        'end_hour': max_hour,
        'include_cbd': include_cbd,
        'agg_mode': agg_mode,
        'aggreg_by': aggreg_by,
        'chart_mode': chart_mode,
        'resolution_mode': resolution_mode,
        'selector_vals': []
    }

    for selector in selectors:
        val_indexes = [selector.options.index(v) for v in selector.value]
        ret_vals['selector_vals'].append([selector.name, val_indexes])

    return ret_vals

def get_new_data():
    data = {'data': json.dumps(controls_vals())}
    resp = requests.get('http://localhost:5000/', data=data).json()
    ret = json.loads(resp)

    return ret['geojson'], ret['timeseries_data']

def clear_chart():
    """Removes rendered lines from main chart"""
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
        datetime_xs = []
        for raw_xs in timeseries_data['xs']:
            datetime_xs.append([datetime.strptime(x, '%Y-%m-%d') for x in raw_xs])

        timeseries_data['xs_datetime'] = datetime_xs

        timeseries_data['line_color'] = chart_palette

        line_opts = {
            'xs': 'xs_datetime',
            'ys': 'ys',
            'line_width': 1.5,
            'line_alpha': .8,
            'line_color': 'line_color',
            'source': ColumnDataSource(timeseries_data),
        }

        global chart_lines
        chart_lines = chart.multi_line(**line_opts)

        chart_tooltips = [
            ("value", "$y{int}"),
            ("Date", "$x{%F}"),
            ("name", "@keys"),
        ]

        chart_labels = [s['title'] for s in conf.selectors]
        chart_by = chart_labels[select_chart_radios.active]

        chart_modes = ["Yearly", "Monthly", "Weekly", "Daily"]
        chart_mode = chart_modes[chart_res_radios.active]

        agg_modes = ['Count', 'Due', 'Paid', 'Penalties', 'Initial Amnt.']
        agg_mode = agg_modes[select_type_radios.active]

        if agg_mode == 'Count':
            of_or_from = 'Of'

        else:
            of_or_from = 'From'

        cumm_modes = ['', 'Cummulative ']
        cumm_mode = cumm_modes[chart_rbg_radios.active]

        title_vals = [cumm_mode, chart_mode, agg_mode, of_or_from, chart_by]

        chart_title.text = '{}{} {} {} Tickets By {}'.format(*title_vals)

        for tool in chart.tools:
            if tool.name == 'chart_hovertool':
                del tool

        hovertool_opts = dict(tooltips=chart_tooltips,
                              formatters={'$x': 'datetime'},
                              line_policy='nearest',
                              mode='mouse',
                              name='chart_hovertool')

        chart_hovertool = HoverTool(**hovertool_opts)
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
    feat_range = range(len(geojson_data['features']))
    data_vals = [geojson_data['features'][i]['properties']['data_val'] for i in feat_range]

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

    elif select_type_radios.active == 1:
        hover_tool.tooltips = [("$ Due", "@data_val")]

    elif select_type_radios.active == 2:
        hover_tool.tooltips = [("$ Paid", "@data_val")]

    elif select_type_radios.active == 3:
        hover_tool.tooltips = [("$ in Penalties", "@data_val")]

    elif select_type_radios.active == 4:
        hover_tool.tooltips = [("$ in Initial Amounts", "@data_val")]

    print("done updating - took {} seconds".format(time_delta))
    update_button.label = "Update"
    update_button.disabled = False

with open('/opt/ticket_viz/data/default_geojson', 'r') as f:
    geo_source = GeoJSONDataSource(geojson=f.read())

start_date = date(*map(int, conf.start_date.split('-')))
end_date = date(*map(int, conf.end_date.split('-')))

date_selector = DateRangeSlider(title='Date', start=start_date,
                                value=(start_date, end_date),
                                end=end_date, step=1,
                                callback_policy="mouseup")

hours_selector = RangeSlider(title="Hours Range", start=0, end=23, value=(0, 23))

selectors = []
for selector_details in conf.selectors:
    column_name = selector_details['column_name']
    title = selector_details['title']

    opts = selector_opts[column_name]

    if 'index_map' in selector_details:
        new_opts = [opts[0]] #include "All"
        for opt in opts[1:]:
            opt_name = ' '.join(opt.split(' ')[:-1])
            opt_count = opt.split(' ')[-1]

            mapped_name = selector_details['index_map'][int(opt_name)]
            opt_text = '{} {}'.format(mapped_name, opt_count)
            new_opts.append(opt_text)

        opts = new_opts

    title_text = '{}: '.format(title)

    selector_params = dict(title=title_text,
                         value=[opts[0]],
                         options=opts,
                         size=4,
                         name=column_name)

    multi_selector = MultiSelect(**selector_params)
    selectors.append(multi_selector)

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
    'tooltips': geomap_tooltips,
    'tools': '',
    'x_axis_type': "mercator",
    'y_axis_type': "mercator",
    'x_range': (-9789724.66045665, -9742970.474323519),
    'y_range': (5107551.543942757, 5164699.247119262),
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

chart_opts = {
    'plot_width':840,
    'plot_height':350,
    'tools':'',
    'x_axis_type': 'datetime',
    'output_backend': 'webgl',
    'title': '',
}

chart = figure(**chart_opts)
chart_title = Title()
chart_title.text = "starting title"
chart.title = chart_title

chart_wheel_zoom = WheelZoomTool()
chart_pan_tool = PanTool()

chart.add_tools(chart_wheel_zoom, chart_pan_tool)
chart.toolbar.active_scroll = chart_wheel_zoom

chart.xaxis.visible = False
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
        labels=["Count", "Due", "Paid", "Penalties", "Initial Amnt"], active=0)

chart_res_div = Div(text="Chart resolution: ")
chart_res_labels = ["Yearly", "Monthly", "Weekly", "Daily (Slow)"]
chart_res_radios = RadioButtonGroup(labels=chart_res_labels, active=1)

chart_rbg_div = Div(text="Chart type: ")
chart_rbg_labels = ["Non-Cummulative", "Cummulative"]
chart_rbg_radios = RadioButtonGroup(labels=chart_rbg_labels, active=0)

chart_rbg_div = Div(text="Chart Lines By: ")
chart_labels = [s['title'] for s in conf.selectors]
select_chart_radios = RadioButtonGroup(labels=chart_labels, active=0)

ticker = LogTicker(desired_num_ticks=8)

color_bar = ColorBar(color_mapper=color_mapper, ticker=ticker, location=(0, 0))
geomap.add_layout(color_bar, 'right')
geomap.right[0].formatter.use_scientific = False

datetime_selectors = [date_selector, hours_selector]
radios_and_divs = [
    select_rbg_div, select_type_radios, chart_rbg_div, select_chart_radios,
    chart_rbg_radios, chart_res_div, chart_res_radios
]

controls = [
    *datetime_selectors, *selectors, *radios_and_divs,
    central_bus_toggle, update_button
]

inputs = widgetbox(*controls, sizing_mode='fixed', width=420)

col2 = column(geomap, chart)
col1 = column(inputs)

l = layout([[col1, col2]], sizing_mode='fixed')

curdoc().add_root(l)
update()
