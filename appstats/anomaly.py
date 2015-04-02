from flask import url_for

from collections import namedtuple


_Anomaly = namedtuple('Anomaly', ['app_id', 'name', 'field'])


class Anomaly(_Anomaly):
    @property
    def url(self):
        return url_for('stats_frontend.apps_info',
                       app_id=self.app_id, name=self.name, _external=True)


