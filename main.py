import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer


def process_msg(message):
    parts = message.split(',')
    msg_dict = {
        "VinVehicle": parts[0],
        "Timestamp": int(parts[1]),
        "Driver": parts[2],
        "Odometer": int(parts[3]),
        "LifeConsumption": int(parts[4]),
        "Position.lon": float(parts[5]),
        "Position.lat": float(parts[6]),
        "Position.altitude": float(parts[7]),
        "Position.heading": float(parts[8]),
        "Position.speed": int(float(parts[9])),
        "Position.satellites": int(float(parts[10]))
    }
    timestamp = datetime.utcfromtimestamp(msg_dict['Timestamp'])  # assume timestamp is UTC
    msg_dict['MessageDate'] = timestamp.isoformat()
    if msg_dict['Position.satellites'] < 3:
        msg_dict["Position.lon"] = -1
        msg_dict["Position.lat"] = -1
    if msg_dict['Position.altitude'] < 0:
        msg_dict['Position.altitude'] = 0
    return msg_dict


if __name__ == '__main__':
    topic = 'VinVehicle'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: x.decode('utf-8'),
        auto_offset_reset='earliest'
    )
    prod = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    futures = []
    count = 1
    for msg in consumer:
        print(f"retrieved message {count} from topic {topic}")
        dict_msg = process_msg(msg.value)
        realtime_msg = {
            "Position.lon": dict_msg["Position.lon"],
            "Position.lat": dict_msg["Position.lat"],
            "Position.altitude": dict_msg["Position.altitude"],
            "Position.heading": dict_msg["Position.heading"],
            "Position.speed": dict_msg["Position.speed"],
            "Position.satellites": dict_msg["Position.satellites"]
        }
        print(f"processed message {count}, sending to batch and realtime topics")
        count = count + 1
        futures.append(prod.send('batch', dict_msg))
        futures.append(prod.send('realtime', realtime_msg))

    for future in futures:
        future.get(timeout=60)

    print(f"Successfully sent {len(futures)} row")
