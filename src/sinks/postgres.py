
# Local
from alerts import send_alert
from sinks.base import BaseSink

class PostgresSink(BaseSink):
    def __init__(self):
        pass

    def write_ohlcv(self, conn, rows):
        insert_query = """
            INSERT INTO public.ohlcv_1m ( symbol, window_start, "open", high, low, "close", volume ) VALUES ( %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (symbol, window_start) DO UPDATE SET
                "open" = EXCLUDED."open",
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                "close" = EXCLUDED."close",
                volume = EXCLUDED.volume;
            """
        cur = conn.cursor()
        try:
            for row in rows:
                # print(row.symbol, row.price, row.quantity, row.timestamp)
                cur.execute(
                    insert_query,
                    (
                        row.symbol, 
                        row.window_start, 
                        row.open, 
                        row.high, 
                        row.low, 
                        row.close, 
                        row.volume
                    ),
                )
            conn.commit()
            print(f"Loaded {len(rows)} candles into ohlcv_1m!")

        except Exception as e:
            print("Loading ohlcv_1m failed: ", e)
            conn.rollback()
        finally:
            cur.close()

    def write_anomaly(self, conn, rows):
        cur = conn.cursor()
        anomaly_count = 0
        try:
            for row in rows:
                cur.execute("""
                    SELECT close, volume FROM ohlcv_1m
                    WHERE symbol = %s
                    ORDER BY window_start DESC
                    LIMIT 20
                """, (row.symbol,))
                history = cur.fetchall()

                if len(history) < 5:
                    continue

                closes = [float(r[0]) for r in history]
                volumes = [float(r[1]) for r in history]

                for metric, values, current in [
                    ("price", closes, float(row.close)),
                    ("volume", volumes, float(row.volume))
                ]:
                    mean = sum(values) / len(values)
                    std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                    if std == 0:
                        continue
                    z = (current - mean) / std
                    if abs(z) > 2.0:
                        cur.execute("""
                            INSERT INTO anomalies (symbol, detected_at, anomaly_type, z_score, ohlcv_ref)
                            VALUES (%s, NOW(), %s, %s, %s)
                        """, (row.symbol, metric, z, row.window_start))
                        anomaly_count += 1
                        send_alert(row.symbol, metric, z, row.window_start)  # use metric not hardcoded

            conn.commit()
            print(f"Detected {anomaly_count} anomalies!")

        except Exception as e:
            print("Anomaly detection failed:", e)
            conn.rollback()
        finally:
            cur.close()