import json, os, argparse, time
from pathlib import Path
from kafka import KafkaProducer
from common import validate

def producer():
    return KafkaProducer(bootstrap_servers=os.getenv('BOOTSTRAP_SERVERS','localhost:9092'),
        acks='all',retries=5,linger_ms=5,
        key_serializer=lambda x:x.encode(),value_serializer=lambda e:json.dumps(e).encode())

def send_sample(topic):
    p=producer()
    try:
        for e in json.loads(Path('data/events.json').read_text()):
            validate(e)
            p.send(topic,key=e['research_group'],value=e).get(timeout=30)
            print('SENT',e['event_id'],flush=True)
        p.flush()
    finally: p.close()

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--topic',required=True)
    send_sample(ap.parse_args().topic)
