
# Local
from alerts import send_alert
from sinks.base import BaseSink

class SnowflakeSink(BaseSink):
    def __init__(self):
        pass

    def write_ohlcv(self, conn, rows):
        pass

    def write_anomaly(self, conn, rows):
        pass