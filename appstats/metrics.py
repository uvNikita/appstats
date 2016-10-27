from functools import wraps

import cantal

from cantal_tools.metrics import appflow

requests = cantal.RequestTracker('requests')
post = cantal.RequestTracker('post')
appflow.ensure_branches('redis')
appflow.ensure_branches('mongo')


def request_tracking_middleware(wsgi_app):
    def middleware(environ, start_response):
        method = environ['REQUEST_METHOD']
        if method == 'POST':
            with requests.request(), post.request():
                return wsgi_app(environ, start_response)
        else:
            with requests.request():
                return wsgi_app(environ, start_response)
    return middleware


def patch_redis(redis, pipeline):
    """Wraps `execute_command` method."""
    redis_real_execute = redis.execute_command
    pipeline_real_execute = pipeline.execute

    @wraps(redis_real_execute)
    def redis_execute_with_metrics(*args, **options):
        with appflow.redis.context():
            return redis_real_execute(*args, **options)

    @wraps(pipeline_real_execute)
    def pipeline_execute_with_metrics(*args, **options):
        with appflow.redis.context():
            return pipeline_real_execute(*args, **options)

    redis.execute_command = redis_execute_with_metrics
    pipeline.execute_command = redis_execute_with_metrics
    return redis


def patch_mongo(mongo):
    """Wraps `_send_message_with_response` method."""
    real_func = mongo._send_message_with_response

    @wraps(real_func)
    def execute_with_metrics(*args, **options):
        with appflow.mongo.context():
            return real_func(*args, **options)

    mongo._send_message_with_response = execute_with_metrics
    return mongo
