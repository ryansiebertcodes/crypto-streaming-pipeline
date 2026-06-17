import json
import websocket
import confluent_kafka


# create kafka producer
producer = confluent_kafka.Producer({'bootstrap.servers': 'localhost:9092'})

def on_message(ws, message):
    print(f"Received: {message}")
    data = json.loads(message)['data']
    parsed_data = {"symbol": data['s'], 
                   "price": data['p'], 
                   "quantity": data['q'], 
                   "timestamp": data['T']
    }
    print(f"Symbol: {parsed_data['symbol']}")
    print(f"Price: {parsed_data['price']}")
    print(f"Quantity: {parsed_data['quantity'] }") 
    print(f"Timestamp: {parsed_data['timestamp'] }") 
    producer.produce("raw-trades", json.dumps(parsed_data))
    producer.flush()

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    producer.flush()
    print("Connection closed")


if __name__ == "__main__":
    # url = "wss://stream.binance.us:9443/ws/btcusdt@trade"
    url = "wss://stream.binance.us:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade"

    # create WebSocketApp pointing at btcusdt@trade
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        url, 
        on_message = on_message, 
        on_close = on_close,
        on_error = on_error,
    )
    ws.run_forever()


# ++Rcv decoded: fin=1 opcode=1 data=b'{"e":"trade","E":1781046262443,"s":"BTCUSDT","t":31385760,"p":"61778.38000000","q":"0.00007000","b":1750691143,"a":1750691128,"T":1781046262442,"m":false,"M":true}'
# {
#    "e":"trade",
#    "E":1781045948490,
#    "s":"BTCUSDT",
#    "t":31385754,
#    "p":"61702.09000000",
#    "q":"0.00008000",
#    "b":1750688969,
#    "a":1750688955,
#    "T":1781045948490,
#    "m":false,
#    "M":true
# }
#   2. Parse the message

#   Each message from Binance comes in as a JSON string. The fields you care about:
#   - s — symbol
#   - p — price
#   - q — quantity
#   - T — trade timestamp (milliseconds)

#   Parse it with json.loads() and pull out those four fields.

#   3. Publish to Kafka

#   Use confluent-kafka (pip install confluent-kafka). The key calls are:
#   - Producer({'bootstrap.servers': 'localhost:9092'}) to create a producer
#   - producer.produce(topic, key, value) to send a message — serialize your dict as a JSON string for the value
#   - producer.flush() periodically to ensure messages are sent

#   4. Structure

#   on_message callback:
#     → parse JSON from Binance
#     → extract the 4 fields into a clean dict
#     → produce to topic 'raw-trades'

#   main:
#     → create Kafka producer
#     → create WebSocketApp pointing at btcusdt@trade
#     → call ws.run_forever()

#   What to look up:
#   - websocket.WebSocketApp docs — focus on run_forever()
#   - confluent_kafka.Producer — specifically the produce() and flush() methods

#   Start with just printing the parsed message to the console before wiring in Kafka — confirms the WebSocket is working
#   before adding complexity.