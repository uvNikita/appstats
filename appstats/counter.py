# encoding: utf-8

import logging
from time import sleep
from calendar import timegm
from datetime import datetime, timedelta, time
from functools import wraps

from pymongo.errors import AutoReconnect

from .util import lock
from .anomaly import Anomaly


log = logging.getLogger(__name__)


def with_rolling_counter_lock(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        key = self._make_key(self.lock_key_format)
        with lock(self.db, key, self.MAX_UPDATE_TIME) as acquired:
            if acquired:
                return func(self, *args, **kwargs)
            else:
                log.warning("Key '{}' is acquired, exiting".format(key))
                return
    return wrapper


class RollingCounter(object):
    """
    The rolling counter, which stores data only
    for the specified time interval.

    Parameters:
      - 'db' -- redis db instance to accumulate statistics data in
      - 'fields' -- list of fields names to track of
      - 'redis_prefix' -- prefix used in each redis key to separate statistics
      data
      - 'stats' -- statistics type id
      - 'interval' -- interval during which the counter stores the data
      - 'secs_per_part' -- accuracy indicator, determine the time range of
      one part in seconds
    """
    REDIS_BUCKET_SIZE = 10000

    last_val_key_format = '%(prefix)s,%(app_id)s,%(name)s,%(interval)s,%(secs_per_part)s,last_val,%(field)s'
    updated_key_format = '%(prefix)s,%(app_id)s,%(name)s,%(interval)s,%(secs_per_part)s,updated,%(field)s'
    app_ids_key_format = '%(prefix)s,%(interval)s,%(secs_per_part)s,app_ids_set'
    names_key_format = '%(prefix)s,%(interval)s,%(secs_per_part)s,%(app_id)s,names_set'
    key_format = '%(prefix)s,%(app_id)s,%(name)s,%(interval)s,%(secs_per_part)s,%(field)s'
    lock_key_format = '%(prefix)s,%(interval)s,%(secs_per_part)s,lock'

    MAX_UPDATE_TIME = 5 * 60  # 5 minutes

    def __init__(self, db, fields, redis_prefix, stats='apps',
                 interval=3600, secs_per_part=60):
        self.db = db
        self.prefix = '%s_%s' % (redis_prefix, stats)
        self.interval = interval
        self.secs_per_part = secs_per_part
        self._num_of_parts = interval // secs_per_part
        self.fields = fields

    def _make_key(self, key_format, **kwargs):
        """
        Return redis key produced by inserting kwargs into given `key_format`.
        Specify format with `prefix`, `interval` and `secs_per_part` values
        taken from counter object.
        """
        kwargs.update(prefix=self.prefix, interval=self.interval,
                      secs_per_part=self.secs_per_part)
        return key_format % kwargs

    def _get_app_ids(self):
        """
        Return all app_ids this counter works with.
        """
        key_app_ids = self._make_key(self.app_ids_key_format)
        for app_id, _ in self.db.zscan_iter(key_app_ids):
            yield app_id

    def _get_names(self, app_id):
        """
        Return all names this counter watching over.
        """
        key_names = self._make_key(self.names_key_format, app_id=app_id)
        for name, _ in self.db.zscan_iter(key_names):
            yield name

    def _remove_old_app_ids(self, latest):
        latest_ts = timegm(latest.utctimetuple())
        key_app_ids = self._make_key(self.app_ids_key_format)
        self.db.zremrangebyscore(key_app_ids, 0, latest_ts)

    def _remove_old_names(self, app_id, latest):
        latest_ts = timegm(latest.utctimetuple())
        key_names = self._make_key(self.names_key_format, app_id=app_id)
        self.db.zremrangebyscore(key_names, 0, latest_ts)

    @with_rolling_counter_lock
    def update(self):
        """
        Actualize counter statistics.
        This method should be called periodically (not necessarily) with
        period less or equal `secs_per_part` for better accuracy.
        """
        log.info(
            "RollingCounter (interval: {interval}) "
            "update was triggered".format(interval=self.interval)
        )
        now_dt = datetime.utcnow()
        now_ts = timegm(now_dt.utctimetuple())
        latest = now_dt - timedelta(days=10)

        self._remove_old_app_ids(latest)
        pl = self.db.pipeline()
        for app_id in self._get_app_ids():
            self._remove_old_names(app_id, latest)
            for name in self._get_names(app_id):
                for field in self.fields:
                    key = self._make_key(self.key_format, name=name,
                                         app_id=app_id, field=field)
                    last_val_key = self._make_key(self.last_val_key_format,
                                                  app_id=app_id, name=name,
                                                  field=field)
                    updated_key = self._make_key(self.updated_key_format,
                                                 app_id=app_id, name=name,
                                                 field=field)

                    if self.db.llen(key) == 0:
                        for i in xrange(self._num_of_parts - 1):
                            self.db.rpush(key, 0)
                        self.db.set(updated_key, now_ts)

                    updated = float(self.db.get(updated_key))
                    last_val = float(self.db.get(last_val_key) or '0.0')
                    passed_time = now_ts - updated

                    # Check whether it is need to be updated
                    if passed_time > self.secs_per_part:
                        num_of_new_parts = int(
                            passed_time // self.secs_per_part)
                        val_per_part = last_val / num_of_new_parts

                        # There is no need to pop and push the same values more
                        # than self._num_of_parts times.
                        num_of_shifts = min(self._num_of_parts, num_of_new_parts)

                        # For each new part perform a shift,
                        # filling a new cell with the value per one part
                        for i in xrange(num_of_shifts):
                            pl.lpop(key)
                            pl.rpush(key, val_per_part)

                        # New last_val = 0
                        pl.set(last_val_key, 0)

                        # Evaluating time correction
                        rest_time = passed_time - num_of_new_parts * self.secs_per_part
                        pl.set(updated_key, now_ts - rest_time)
                    if len(pl) > self.REDIS_BUCKET_SIZE:
                        pl.execute()

    def get_vals(self):
        """
        Return all data counter has.
        Result format example:
            {'app_id1': {'name1': {'field1': 1, 'field2': 2}}}
        """
        pl = self.db.pipeline()
        app_id_names = {app_id: list(self._get_names(app_id))
                        for app_id in list(self._get_app_ids())}
        for app_id, names in app_id_names.iteritems():
            for name in names:
                for field in self.fields:
                    key = self._make_key(self.key_format, name=name,
                                         app_id=app_id, field=field)
                    last_val_key = self._make_key(self.last_val_key_format,
                                                  app_id=app_id, name=name,
                                                  field=field)
                    pl.get(last_val_key)
                    pl.lrange(key, 0, -1)
        pl_res = pl.execute()
        res = {}
        for app_id, names in app_id_names.iteritems():
            res[app_id] = {}
            for name in names:
                counts = {}
                for field in self.fields:
                    last_val = float(pl_res.pop(0) or '0.0')
                    count = sum(map(float, pl_res.pop(0)))
                    count += last_val
                    counts[field] = count
                res[app_id][name] = counts
        return res

    def incrby(self, app_id, name, field, increment):
        """
        Add `increment` to value of a count
        specified by `app_id`, `name` and `field`
        """
        if ',' in name:
            raise ValueError("Name can't contain ',' (comma)")
        if ',' in app_id:
            raise ValueError("App_id can't contain ',' (comma)")
        if field not in self.fields:
            return

        pl = self.db.pipeline()
        key_app_ids = self._make_key(self.app_ids_key_format)
        key_names = self._make_key(self.names_key_format, app_id=app_id)
        last_val_key = self._make_key(self.last_val_key_format, app_id=app_id,
                                      name=name, field=field)
        now = timegm(datetime.utcnow().utctimetuple())
        pl.zadd(key_app_ids, now, app_id)
        pl.zadd(key_names, now, name)
        pl.incrbyfloat(last_val_key, increment)
        pl.execute()


def with_periodic_counter_lock(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        key = self._make_key(self.lock_key_format)
        with lock(self.redis_db, key, self.MAX_UPDATE_TIME) as acquired:
            if acquired:
                return func(self, *args, **kwargs)
            else:
                log.warning("Key '{}' is acquired, exiting".format(key))
                return
    return wrapper


class PeriodicCounter(object):
    """
    The PeriodicCounter stores data, accumulated during strict intervals.
    E.g. if divider = 1, counter stores data that came from 00:00 to 00:59.

    Parameters:
      - 'divider' -- hour divider, specifies interval. E.g. if divider = 3,
      interval = 60 / 3 = 20 minutes. 1 <= divider <= 60
      - 'redis_db' -- redis db instance to temporary accumulate information in
      - 'mongo_db' -- mongo db instance to store statistics in
      - 'fields' -- list of fields names to track of
      - 'redis_prefix' -- prefix used in each redis key to separate statistics
      data
      - 'stats' -- statistics type id
      - 'period' -- interval in hours during which the counter stores the data.
      All data older than this value will be removed.
      Default value is 30 * 24 = 720 (30 days).
    """

    key_format = '%(prefix)s,periodic,%(divider)s,%(app_id)s,%(name)s,%(field)s'
    prev_upd_key_format = '%(prefix)s,periodic,%(divider)s,prev_upd'
    app_ids_key_format = '%(prefix)s,periodic,%(divider)s,app_ids_set'
    names_key_format = '%(prefix)s,periodic,%(divider)s,%(app_id)s,names_set'
    lock_key_format = '%(prefix)s,periodic,%(divider)s,lock'

    MAX_MONGO_RETRIES = 3
    MAX_PASSED_INTERVALS = 5
    MAX_UPDATE_TIME = 5 * 60  # 5 minutes

    def __init__(self, divider, redis_db, mongo_db, fields,
                 redis_prefix, stats='apps', period=720):
        self.redis_db = redis_db
        self.fields = fields
        self.collection = mongo_db['appstats_%s_periodic-%u' % (stats, divider)]
        self.prefix = '%s_%s' % (redis_prefix, stats)
        self.divider = divider
        self.period = period
        self.interval = 60 / divider

    def _get_app_ids(self):
        key_app_ids = self._make_key(self.app_ids_key_format)
        for app_id, _ in self.redis_db.zscan_iter(key_app_ids):
            yield app_id

    def _get_names(self, app_id):
        key_names = self._make_key(self.names_key_format, app_id=app_id)
        for name, _ in self.redis_db.zscan_iter(key_names):
            yield name

    def _make_key(self, key_format, **kwargs):
        kwargs.update(prefix=self.prefix, divider=self.divider)
        return key_format % kwargs

    def _insert_docs(self, docs):
        if not docs:
            return
        log.debug("Going to insert {} docs in mongo".format(len(docs)))
        # Make MAX_MONGO_RETRIES tries to insert docs
        tries = self.MAX_MONGO_RETRIES
        while True:
            try:
                self.collection.insert(docs)
                break
            except AutoReconnect:
                log.warn("AutoReconnect exception while inserting "
                         "docs in mongo")
                # Last try, raise exception
                if tries <= 0:
                    raise
                tries -= 1
                sleep(0.1)

    def incrby(self, app_id, name, field, increment):
        if ',' in name:
            raise ValueError("Name can't contain ',' (comma)")
        if ',' in app_id:
            raise ValueError("App_id can't contain ',' (comma)")
        if field not in self.fields:
            return

        pl = self.redis_db.pipeline()
        key_app_ids = self._make_key(self.app_ids_key_format)
        key_names = self._make_key(self.names_key_format, app_id=app_id)
        key = self._make_key(self.key_format, app_id=app_id, name=name,
                             field=field)

        now = timegm(datetime.utcnow().utctimetuple())
        pl.zadd(key_app_ids, now, app_id)
        pl.zadd(key_names, now, name)
        pl.incrbyfloat(key, increment)
        pl.execute()

    def _remove_old_app_ids(self, latest):
        latest_ts = timegm(latest.utctimetuple())
        key_app_ids = self._make_key(self.app_ids_key_format)
        self.redis_db.zremrangebyscore(key_app_ids, 0, latest_ts)

    def _remove_old_names(self, app_id, latest):
        latest_ts = timegm(latest.utctimetuple())
        key_names = self._make_key(self.names_key_format, app_id=app_id)
        self.redis_db.zremrangebyscore(key_names, 0, latest_ts)

    @with_periodic_counter_lock
    def update(self):
        prev_upd_key = self._make_key(self.prev_upd_key_format)
        prev_upd = self.redis_db.get(prev_upd_key)
        log.info(
            "PeriodicCounter (collection: {collection}) "
            "update was triggered, previous update: {prev}".format(
                collection=self.collection.name, prev=prev_upd,
            )
        )

        # Get current utc datetime rounded to interval
        now = datetime.utcnow()
        new_time = time(hour=now.hour,
                        minute=((now.minute // self.interval) * self.interval))
        now = datetime.combine(now.date(), new_time)
        if prev_upd:
            prev_upd = int(prev_upd)  # Get previous timestamp
            prev_upd = datetime.utcfromtimestamp(prev_upd)
        else:
            # If there isn't prev_upd in redis,
            # use 'one interval before current time' variable instead
            prev_upd = now - timedelta(minutes=self.interval)
        delta = now - prev_upd
        passed_intervals = int(delta.total_seconds() / 60 / self.interval)
        if passed_intervals == 0:
            # Too early, exiting
            return

        # Quick fix for case, when many intervals have passed
        num_intervals = min(passed_intervals, self.MAX_PASSED_INTERVALS)

        pl = self.redis_db.pipeline()
        docs = []
        latest = now - timedelta(days=10)

        self._remove_old_app_ids(latest)
        for app_id in self._get_app_ids():
            self._remove_old_names(app_id, latest)
            for name in self._get_names(app_id):
                doc = dict(name=name, app_id=app_id, date=now)
                for field in self.fields:
                    key = self._make_key(self.key_format, app_id=app_id,
                                         name=name, field=field)
                    val = self.redis_db.get(key)
                    val = float(val) if val else 0.0
                    # Reduce value by val (pipelined)
                    pl.incrbyfloat(key, -val)
                    val_per_interval = val / passed_intervals
                    doc[field] = val_per_interval
                docs.append(doc)
        try:
            self._insert_docs(docs)
            pl.execute()
            prev_upd = timegm(now.utctimetuple())
            self.redis_db.set(prev_upd_key, prev_upd)

            oldest_date = now - timedelta(hours=self.period)
            self.collection.remove({'date': {'$lte': oldest_date}})

            # For each passed interval
            # add separate doc with changed date
            for offset_scale in xrange(1, num_intervals):
                for doc in docs:
                    offset = self.interval * offset_scale
                    date = now - timedelta(minutes=offset)
                    doc['date'] = date
                    del doc['_id']
                self._insert_docs(docs)
        except AutoReconnect as e:
            log.warning("Failed to update counters: {}".format(e))
            pl.reset()

    def find_anomalies(self, ref_hours, check_hours, sensitivity):
        def get_avg_data(start_date, end_date):
            groupper = {'_id': {'app_id': '$app_id', 'name': '$name'}}
            for field in self.fields:
                groupper[field] = {'$avg': '$' + field}
            pipeline = [
                {'$match': {'date': {'$gt': start_date, '$lt': end_date}}},
                {'$group': groupper}
            ]
            raw_results = self.collection.aggregate(pipeline)['result']
            results = {}
            for raw_result in raw_results:
                app_id = raw_result['_id']['app_id']
                name = raw_result['_id']['name']
                for field in self.fields:
                    results[app_id, name, field] = raw_result.get(field)
            return results

        ref_end_date = datetime.utcnow() - timedelta(hours=check_hours)
        ref_start_date = ref_end_date - timedelta(hours=ref_hours)
        ref_data = get_avg_data(ref_start_date, ref_end_date)

        check_start_date = ref_end_date
        check_end_date = datetime.utcnow()
        check_data = get_avg_data(check_start_date, check_end_date)

        anomalies = []
        for (app_id, name, field), ref_val in ref_data.items():
            check_val = check_data.get((app_id, name, field), 0.0)
            if ref_val == 0.0:
                # if check_val > 0.0:
                #     anomaly = Anomaly(app_id=app_id, name=name, field=field)
                #     anomalies.append(anomaly)
                continue
            error = abs(ref_val - check_val) / ref_val
            if error >= 1.0 - sensitivity:
                anomaly = Anomaly(app_id=app_id, name=name, field=field)
                anomalies.append(anomaly)
        return anomalies
