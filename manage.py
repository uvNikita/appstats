# encoding: utf-8
import logging

from email.utils import make_msgid
from email.header import Header
from email.mime.text import MIMEText

from smtplib import SMTP
from datetime import datetime, timedelta, date

from flaskext.script import Manager
from pymongo import ASCENDING

from appstats.app import app, apps_last_hour_counter, apps_last_day_counter
from appstats.app import tasks_last_hour_counter, tasks_last_day_counter
from appstats.app import apps_periodic_counters, tasks_periodic_counters
from appstats.app import apps_counters, tasks_counters
from appstats.app import REDIS_PREFIX, redis_db, mongo_db, fields

from appstats.util import calc_aver_data, data_to_flat_form, log_time_call

manager = Manager(app)


@manager.option('-d', '--days', dest='days', type=int, default=182)
@manager.option('-s', '--stats', required=True, dest='stats',
                choices=['apps', 'tasks'], help='Stats to strip')
def strip_db(days, stats):
    """ Remove data older then specified number of days from db. """
    oldest_date = datetime.utcnow() - timedelta(days)
    if stats == 'apps':
        periodic_counters = apps_periodic_counters
    elif stats == 'tasks':
        periodic_counters = apps_periodic_counters
    for periodic_counter in periodic_counters:
        periodic_counter.collection.remove({'date': {'$lt': oldest_date}})


@manager.command
def clear():
    """ Delete all data from redis and mongo dbs. """
    # Flush all redis records with appstats prefix
    keys = redis_db.keys('%s*' % REDIS_PREFIX)
    if keys:
        redis_db.delete(*keys)
    # Drop mongo 'cache' collection
    mongo_db.drop_collection('appstats_docs')
    mongo_db.drop_collection('appstats_tasks_docs')
    # Drop mongo events collection
    mongo_db.drop_collection('appstats_events')
    # Drop all mongo periodic stats collections
    for periodic_counter in apps_periodic_counters + tasks_periodic_counters:
        mongo_db.drop_collection(periodic_counter.collection)


@manager.option('-s', '--stats', required=True, dest='stats',
                choices=['apps', 'tasks'], help='Statistics to update')
@log_time_call(logging.INFO)
def update_counters(stats):
    """ Update all counters """
    if stats == 'apps':
        counters = apps_counters
        periodic_counters = apps_periodic_counters
    elif stats == 'tasks':
        counters = tasks_counters
        periodic_counters = tasks_periodic_counters
    # Ensuring indexes
    for counter in periodic_counters:
        counter.collection.ensure_index([('app_id', ASCENDING),
                                         ('name', ASCENDING),
                                         ('date', ASCENDING)],
                                        cache_for=3600)
        counter.collection.ensure_index('date', cache_for=3600)
    for counter in counters:
        counter.update()


@manager.option('-s', '--stats', required=True, dest='stats',
                choices=['apps', 'tasks'], help='Statistics to update')
@log_time_call(logging.INFO)
def update_cache(stats):
    """ Update cache """
    if stats == 'apps':
        collection = mongo_db['appstats_docs']
        last_hour_counter = apps_last_hour_counter
        last_day_counter = apps_last_day_counter
    elif stats == 'tasks':
        collection = mongo_db['appstats_tasks_docs']
        last_hour_counter = tasks_last_hour_counter
        last_day_counter = tasks_last_day_counter
    # Ensuring indexes
    collection.ensure_index([('app_id', ASCENDING), ('name', ASCENDING)],
                            cache_for=3600)

    hour_data = last_hour_counter.get_vals()
    day_data = last_day_counter.get_vals()
    hour_aver_data = calc_aver_data(hour_data, last_hour_counter.interval)
    day_aver_data = calc_aver_data(day_data, last_day_counter.interval)

    docs = data_to_flat_form(hour_data, hour_aver_data,
                             day_data, day_aver_data,
                             last_hour_counter.fields)
    # Replace with new data
    collection.remove()
    if docs:
        collection.insert(docs.values())


def send_email(from_email, to_emails, content, subject):
    login = app.config['SMTP_LOGIN']
    password = app.config['SMTP_PASSWORD']
    server = SMTP(app.config['SMTP_SERVER'])
    if login:
        server.login(login, password)
    app.logger.info("sending '{}' to: {}".format(subject, to_emails))
    email = MIMEText(content, 'html')
    email['Subject'] = Header(subject)
    email['Message-ID'] = make_msgid()
    email['To'] = Header(to_emails[0])
    try:
        server.sendmail(from_email, to_emails, email.as_string())
    except Exception as e:
        app.logger.exception(e)


@manager.option('-m', '--mode', dest='mode',
                choices=['console', 'email'], default='email',
                help='Print results to console or send email')
@manager.option('-s', '--sensitivity', required=True,
                help='Sensitivity of anomaly deviation. (0.0 -- 1.0)')
@manager.option('-c', '--checkhours', required=True, dest='check_hours',
                help='Hours to check for anomalies')
@manager.option('-r', '--refhours', required=True, dest='ref_hours',
                help='Reference hours used as standard')
def find_anomalies(ref_hours, check_hours, sensitivity, mode):
    ref_hours = int(ref_hours)
    check_hours = int(check_hours)
    sensitivity = float(sensitivity)
    assert ref_hours > 0
    assert check_hours > 0
    assert ref_hours > check_hours
    assert 0.0 < sensitivity < 1.0

    def anomaly_text(anomaly):
        field_name = next(f['name'] for f in fields if f['key'] == anomaly.field)
        return '{url} ({field})'.format(url=anomaly.url, field=field_name)

    counter = apps_periodic_counters[-1]
    anomalies = counter.find_anomalies(int(ref_hours),
                                       int(check_hours),
                                       float(sensitivity))
    if not anomalies:
        return

    if mode == 'console':
        message = '\n'.join(map(anomaly_text, anomalies))
        print message
    else:
        if app.config['INFO_EMAILS']:
            message = '\n'.join(map(anomaly_text, anomalies))
            subject = 'Appstats anomalies: {}'.format(date.today())
            send_email(app.config['APP_EMAIL'],
                       app.config['INFO_EMAILS'],
                       message, subject)


if __name__ == '__main__':
    manager.run()
