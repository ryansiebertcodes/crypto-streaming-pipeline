from abc import ABC, abstractmethod

class BaseSink(ABC):
    @abstractmethod
    def write_ohlcv(self, conn, rows):
        pass

    @abstractmethod
    def write_anomaly(self, conn, rows):
        pass
