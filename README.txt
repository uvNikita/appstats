Install requirements:
    $ pip install -r requirements.txt

For anomaly detection:
    $ sudo add-apt-repository ppa:marutter/rrutter
    $ sudo apt-get update
    $ sudo apt-get install r-base-core, build-essential, curl, libxml2-dev, libcurl4-gnutls-dev, libssl-dev
    $ /usr/lib/R/bin/R --slave --no-restore --file=init.R


Default settings file: appstats/config.py

Settings files:
    1. path from environment variable 'APPSTATS_SETTINGS'
    2. /etc/appstats.cfg
    3. ~/.appstats.cfg

Run service:
    $ gunicorn --daemon --workers=8 appstats.app:app.wsgi_app

Cron command:
    */2 * * * * user python /path/to/appstats/manage.py update_cache -s 'apps'
    */2 * * * * user python /path/to/appstats/manage.py update_cache -s 'tasks'
    */10 * * * * user /usr/lib/R/bin/R --slave --no-restore --file=/path/to/appstats/anomalies.R --args mongo_host:port mongo_db app1 app2
    * * * * * user python /path/to/appstats/manage.py update_counters -s 'apps'
    * * * * * user python /path/to/appstats/manage.py update_counters -s 'tasks'
