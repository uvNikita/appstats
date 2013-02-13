# encoding: utf-8

from copy import deepcopy
from os.path import expanduser
from operator import itemgetter
from datetime import datetime
from collections import OrderedDict

import redis
from flask import Flask, abort, redirect, render_template, request, url_for
from pymongo import Connection, ASCENDING, DESCENDING
from werkzeug.wsgi import ClosingIterator

from .util import current_url, get_chart_info
from .counter import RollingCounter, PeriodicCounter
from .filters import json_filter, time_filter, count_filter, default_filter
from .filters import pretty_hours_filter


app = Flask(__name__)
app.config.from_object('appstats.config')
if not app.config.from_envvar('APPSTATS_SETTINGS', silent=True):
    app.config.from_pyfile('/etc/appstats.cfg', silent=True)
    app.config.from_pyfile(expanduser('~/.appstats.cfg'), silent=True)

APPLICATIONS = OrderedDict(app.config['APPLICATIONS'])

time_fields = deepcopy(app.config['TIME_FIELDS'])
fields = app.config['FIELDS'] + time_fields
if 'NUMBER' not in [field['key'] for field in fields]:
    fields.insert(0, dict(key='NUMBER', name='NUMBER',
                          format=None, visible=True))
fields_keys = [field['key'] for field in fields]
visible_fields = filter(itemgetter('visible'), fields)

redis_db = redis.Redis(host=app.config['REDIS_HOST'],
                       port=app.config['REDIS_PORT'],
                       db=app.config['REDIS_DB'])
REDIS_PREFIX = 'appstats'

mongo_conn = Connection(host=app.config['MONGO_URI'], network_timeout=30,
                        connecttimeoutms=60000, _connect=False)
mongo_db = mongo_conn[app.config['MONGO_DB_NAME']]

last_hour_counter = RollingCounter(db=redis_db, fields=fields_keys,
                                   redis_prefix=REDIS_PREFIX)
last_day_counter = RollingCounter(db=redis_db, fields=fields_keys,
                                  redis_prefix=REDIS_PREFIX, interval=86400,
                                  secs_per_part=3600)
rolling_counters = [last_hour_counter, last_day_counter]

periodic_counters = []
# Very accurate, 6 hours counter with 1 min intervals
periodic_counters.append(PeriodicCounter(
    divider=60, redis_db=redis_db,
    mongo_db=mongo_db, fields=fields_keys,
    redis_prefix=REDIS_PREFIX, period=6))
# Middle accurate, 6 days(144 hours) counter with 10 min intervals
periodic_counters.append(PeriodicCounter(
    divider=6, redis_db=redis_db,
    mongo_db=mongo_db, fields=fields_keys,
    redis_prefix=REDIS_PREFIX, period=144))
# Low accurate, half-year(182 * 24 = 4368) counter with 60 min intervals
periodic_counters.append(PeriodicCounter(
    divider=1, redis_db=redis_db,
    mongo_db=mongo_db, fields=fields_keys,
    redis_prefix=REDIS_PREFIX, period=4368))
periodic_counters = sorted(periodic_counters, key=lambda c: c.period)

counters = rolling_counters + periodic_counters

ROWS_LIMIT_OPTIONS = [10, 25, 50]
INFO_HOURS_OPTIONS = [6, 12, 24, 144, 720]


app.jinja_env.filters['json'] = json_filter
app.jinja_env.filters['time'] = time_filter
app.jinja_env.filters['count'] = count_filter
app.jinja_env.filters['default'] = default_filter
app.jinja_env.filters['pretty_hours'] = pretty_hours_filter

app.jinja_env.globals['current_url'] = current_url


def add_data_middleware(wsgi_app):
    def inner(environ, start_response):
        iterator = wsgi_app(environ, start_response)
        stats = environ.get('appstats.stats')
        if not stats:
            return iterator
        return ClosingIterator(iterator, lambda: add_stats(stats, counters))
    return inner
app.wsgi_app = add_data_middleware(app.wsgi_app)


def add_stats(stats, counters):
    app.logger.debug("Adding new stats: \n %s", stats)
    for app_id in stats:
        for name, counts in stats[app_id].iteritems():
            if not 'NUMBER' in counts:
                for counter in counters:
                    counter.incrby(app_id, name, 'NUMBER', 1)
            for field, val in counts.iteritems():
                for counter in counters:
                    counter.incrby(app_id, name, field, val)


@app.context_processor
def add_nav_list():
    """
    nav_list structure:
    {
        'name': 'AppStats',
        'url': '/',
        'rates': [
            {
                'name': 'Application',
                'url': '/appstats/',
                'active': True
            }
        ],
        'apps_list':{
            'title':'App1',
            'apps':{
                'App1':'/url/1/',
                'App2':'/url/2/'
            }
        }
    }
    """
    app_id = request.view_args.get('app_id')
    assert app_id in APPLICATIONS, "No app_id in args!"

    app_name = APPLICATIONS[app_id]
    apps = {
        def_app_name: current_url(app_id=def_app_id)
        for def_app_id, def_app_name
        in APPLICATIONS.iteritems()}
    rates = []
    rates.append(dict(name='Application',
                      url=url_for('appstats', app_id=app_id),
                      active=True))
    rates.append(dict(name='Task queue',
                      url='',
                      active=False))
    rates.append(dict(name='Logs',
                      url='',
                      active=False))
    rates.append(dict(name='Tracing',
                      url='',
                      active=False))
    nav_list = dict(name='AppStats', url=url_for('dashboard'),
                    rates=rates, apps_list=dict(title=app_name, apps=apps))

    return dict(nav_list=nav_list)


@app.route('/')
def dashboard():
    return redirect(url_for('appstats'))


@app.route('/appstats/', defaults={'app_id': APPLICATIONS.keys()[0]})
@app.route('/appstats/<app_id>/')
def appstats(app_id):
    if app_id not in APPLICATIONS:
        abort(404)

    sort_by_field = request.args.get('sort_by_field', 'NUMBER')
    if (sort_by_field not in (f['key'] for f in visible_fields)
        and sort_by_field != 'name'):
        abort(404)

    sort_by_period = request.args.get('sort_by_period', 'hour')

    rows_limit = request.args.get('rows', ROWS_LIMIT_OPTIONS[0], int)
    if rows_limit not in ROWS_LIMIT_OPTIONS:
        rows_limit = ROWS_LIMIT_OPTIONS[0]

    selected_field = request.args.get('selected_field', 'NUMBER')
    if selected_field not in (f['key'] for f in visible_fields):
        abort(404)

    docs = mongo_db.appstats_docs.find({'app_id': app_id})
    if sort_by_field == 'name':
        docs = docs.sort('name')
    else:
        sort_by = '%s_%s' % (sort_by_field, sort_by_period)
        docs = docs.sort(sort_by, DESCENDING)
    docs = docs.limit(rows_limit)

    return render_template('appstats.jinja', sort_by_field=sort_by_field,
                           app_id=app_id, fields=visible_fields,
                           sort_by_period=sort_by_period, docs=docs,
                           rows_limit=rows_limit,
                           rows_limit_options=ROWS_LIMIT_OPTIONS,
                           selected_field=selected_field)


@app.route('/info/<app_id>/<name>/')
def info_page(app_id, name):
    if app_id not in APPLICATIONS:
        abort(404)
    hours = request.args.get('hours', INFO_HOURS_OPTIONS[0], int)
    if hours not in INFO_HOURS_OPTIONS:
        hours = INFO_HOURS_OPTIONS[0]

    num_data, time_data = get_chart_info(periodic_counters, time_fields,
                                         app_id, name, hours)

    # Get all names from time_fields and use them as labels
    time_labels = [f['name'] for f in time_fields]

    doc = mongo_db.appstats_docs.find_one(
        {'app_id': app_id, 'name': name}) or {}

    return render_template('info_page.jinja', fields=visible_fields, doc=doc,
                           info_hours_options=INFO_HOURS_OPTIONS,
                           num_data=num_data, name=name, hours=hours,
                           time_labels=time_labels, time_data=time_data)


@app.route('/add/', methods=['GET'])
def add_page_help():
    return render_template('add_page_help.jinja')


@app.route('/add/', methods=['POST'])
def add_page():
    stats = request.json
    request.environ['appstats.stats'] = stats
    return 'ok'


@app.route('/add_event/', methods=['GET'])
def add_event_page_help():
    return render_template('add_event_page_help.jinja')


@app.route('/add_event/', methods=['POST'])
def add_event_page():
    events = request.json
    if events:
        docs = [{'app_id': event['app_id'], 'title': event['title'],
                 'date': datetime.utcfromtimestamp(event['timestamp']),
                 'descr': event['descr']} for event in events]
        app.logger.debug("Adding new events: \n %s", docs)
        mongo_db.appstats_events.insert(docs)
    mongo_db.appstats_events.ensure_index([('date', ASCENDING),
                                           ('app_id', ASCENDING)],
                                          ttl=3600)
    return 'ok'
