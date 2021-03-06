REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB = 0

MONGO_URI = 'mongodb://127.0.0.1:27017'
MONGO_DB_NAME = 'appstats'

APP_EMAIL = 'appstats@mail.com'
INFO_EMAILS = []

SMTP_LOGIN = None
SMTP_PASSWORD = None
SMTP_SERVER = None

# APPLICATIONS = (('key1', 'name1'), ('key2', 'name2'))
APPLICATIONS = (('prom.ua', 'Prom.ua'),
                ('tiu.ru', 'Tiu.ru'),
                ('deal.by', 'Deal.by'))

FIELDS = [
    dict(key='NUMBER',    name='NUMBER',  format='count',   visible=True),
    dict(key='cpu_time',  name='CPU',     format='time',    visible=True),
]

TIME_FIELDS = [
    dict(key='real_time',      name='TOTAL',         format='time', visible=True),
    dict(key='memc:duration',  name='MEMC',          format='time', visible=True),
    dict(key='redis:duration', name='REDIS',         format='time', visible=True),
    dict(key='solr:duration',  name='SOLR',          format='time', visible=True),
    dict(key='es:duration',    name='ELASTICSEARCH', format='time', visible=True),
    dict(key='sql:duration',   name='SQL',           format='time', visible=True),
]
