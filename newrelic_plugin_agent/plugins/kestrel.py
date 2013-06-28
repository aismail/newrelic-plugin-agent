"""
Redis plugin polls Redis for stats

"""
import logging
import time

import requests

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Kestrel(base.Plugin):

    GUID = 'com.meetme.newrelic_kestrel_agent'

    DEFAULT_HOST = 'localhost'
    DEFAULT_ADMIN_PORT = 22133
    DEFAULT_NAME = 'kestrel'

    def poll(self):
        """This method is called after every sleep interval. If the intention
        is to use an IOLoop instead of sleep interval based daemon, override
        the run method.

        """
        LOGGER.info('Polling Kestrel')
        start_time = time.time()

        for server in self.config:
            host = server.get('host', self.DEFAULT_HOST)
            admin_port = int(server.get('port', self.DEFAULT_ADMIN_PORT))

            # Fetch kestrel stats from HTTP endpoint of Kestrel admin server
            url = 'http://%s:%d/stats.json' % (host, admin_port)
            try:
                kestrel_stats = self.requests.get(url).json
            except:
                LOGGER.exception("Failed to get Kestrel stats from %s" % url)

            self._parse_kestrel_stats(kestrel_stats)

        # Create all of the metrics
        LOGGER.info('Polling complete in %.2f seconds',
                    time.time() - start_time)

    def _parse_kestrel_stats(self, kestrel_stats):
        gauges = kestrel_stats.get('gauges', {})
        for gauge_name, gauge_value in gauges.iteritems():
            if gauge_name.startswith('q/') and gauge_name.endswith('/items'):
                self.add_gauge_value(gauge_name, 'items', int(gauge_value))
