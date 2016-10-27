import cantal

requests = cantal.RequestTracker('requests')
post = cantal.RequestTracker('post')
adding_stats_flow = cantal.Fork(['app', 'incrby', 'redis'],
                                state='appstats.adding_stats')
request_flow = cantal.Fork(['app', 'mongo'],
                           state='appstats.processing_request')


def request_tracking_middleware(wsgi_app):
    def middleware(environ, start_response):
        method = environ['REQUEST_METHOD']
        if method == 'POST':
            with requests.request(), post.request(), adding_stats_flow.context():
                adding_stats_flow.app.enter()
                return wsgi_app(environ, start_response)
        else:
            with requests.request(), request_flow.context():
                request_flow.app.enter()
                return wsgi_app(environ, start_response)
    return middleware


