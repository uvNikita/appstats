# encoding: utf-8
import logging

from time import mktime, time
from functools import wraps
from datetime import datetime, timedelta

import pytz
from flask import request, url_for


log = logging.getLogger(__name__)


def current_url(**updates):
    kwargs = request.view_args.copy()
    kwargs.update(request.args)
    kwargs.update(updates)
    return url_for(request.endpoint, **kwargs)


def get_chart_info(periodic_counters, time_fields, app_id, name, hours, anomalies_coll=None):
    def date_to_timestamp(date):
        date = date.replace(tzinfo=pytz.utc)
        return mktime(date.timetuple()) * 1000

    # Starting datetime of needed data
    starting_from = datetime.utcnow() - timedelta(hours=hours)
    # Choosing the most suitable, accurate counter based on given hours
    counter = None
    for periodic_counter in periodic_counters:
        if hours <= periodic_counter.period:
            counter = periodic_counter
            break
    # If there isn't suitable counter,
    # take the last one (contains the most full data)
    if not counter:
        counter = periodic_counters[-1]
    docs = counter.collection.find({'app_id': app_id, 'name': name,
                                    'date': {'$gt': starting_from}})
    docs = list(docs.sort('date'))
    # Prepare list of rows for each time_field
    time_data = [[] for _ in time_fields]
    num_data = []
    # If docs is empty, return zero value on current datetime.
    if not docs:
        date = date_to_timestamp(datetime.utcnow())
        num_data = [[date, 0]]
        time_data = [[[date, 0]]]
        anomalies_data = []
        return num_data, time_data, anomalies_data
    # For each doc localize date, transform timestamp from seconds to
    # milliseconds and append list [date, value] to data
    for doc in docs:
        date = date_to_timestamp(doc['date'])
        if doc['NUMBER'] == 0:
            num_data.append([date, None])
            for row in time_data:
                row.append([date, None])
            continue

        req_per_sec = float(doc['NUMBER']) / (counter.interval * 60)
        num_point = [date, req_per_sec]
        num_data.append(num_point)

        for i, time_field in enumerate(time_fields):
            key = time_field['key']
            value = doc.get(key, 0)
            time_data[i].append([date, float(value) / doc['NUMBER'] * 1000])

    if anomalies_coll:
        try:
            anomalies = next(anomalies_coll.find({'name': name, 'app_id': app_id}))
            anomalies = anomalies['anomalies']
        except StopIteration:
            anomalies = []
        anomalies_data = map(date_to_timestamp, anomalies)
    else:
        anomalies_data = []
    return num_data, time_data, anomalies_data


def calc_aver_data(data, interval):
    """ Calculate average data based on ``interval`` """
    aver_data = {}
    for app_id in data:
        aver_data[app_id] = {}
        for name, counts in data[app_id].iteritems():
            req_count = counts['NUMBER']
            aver_counts = {}
            if req_count == 0:
                for field in counts:
                    aver_counts[field] = None
                continue
            for field in counts:
                if field == 'NUMBER':
                    # insert requests per second
                    aver_counts[field] = float(counts[field]) / interval
                else:
                    aver_counts[field] = counts[field] / req_count
            aver_data[app_id][name] = aver_counts
    return aver_data


def data_to_flat_form(hour_data, hour_aver_data,
                      day_data, day_aver_data, fields):
    """ Transform data into flat form """
    docs = {}

    for app_id in hour_data:
        for name in hour_data[app_id]:
            doc = docs.setdefault((app_id, name), dict(app_id=app_id, name=name))
            for field in fields:
                key = '%s_%s' % (field, 'hour')
                doc[key] = hour_data[app_id][name][field]

        for name in hour_aver_data[app_id]:
            doc = docs.setdefault((app_id, name), dict(app_id=app_id, name=name))
            for field in fields:
                key = '%s_%s' % (field, 'hour_aver')
                doc[key] = hour_aver_data[app_id][name][field]

    for app_id in day_data:
        for name in day_data[app_id]:
            doc = docs.setdefault((app_id, name), dict(app_id=app_id, name=name))
            for field in fields:
                key = '%s_%s' % (field, 'day')
                doc[key] = day_data[app_id][name][field]

        for name in day_aver_data[app_id]:
            doc = docs.setdefault((app_id, name), dict(app_id=app_id, name=name))
            for field in fields:
                key = '%s_%s' % (field, 'day_aver')
                doc[key] = day_aver_data[app_id][name][field]
    return docs


def log_time_call(func, log_level=logging.DEBUG):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        secs = time() - start_time
        log.log(
            log_level,
            "'{func}(args={args}, kwargs={kwargs})' "
            "took {secs:0.2f} secs to execute".format(
                func=func.__name__, args=args, kwargs=kwargs, secs=secs
            )
        )
        return result
    return wrapper
