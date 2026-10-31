import argparse, json, os, time
from kafka import KafkaConsumer
from common import Aggregator, save

def consume(topic,group,expected=10,timeout=90):
    c=KafkaConsumer(topic,bootstrap_servers=os.getenv('BOOTSTRAP_SERVERS','localhost:9092'),
        group_id=group,auto_offset_reset='earliest',enable_auto_commit=False)
    a=Aggregator(); bad=[]; deadline=time.monotonic()+timeout
    try:
        while len(a.events)<expected and time.monotonic()<deadline:
            for records in c.poll(timeout_ms=500).values():
                for r in records:
                    try:
                        e=json.loads(r.value); a.add(e)
                        print('RECEIVED',r.partition,r.offset,e['event_id'],flush=True)
                    except (ValueError,TypeError,KeyError) as exc:
                        bad.append({'partition':r.partition,'offset':r.offset,'error':str(exc)})
        result=a.result(); save('results/consumer_results.json',result)
        save('results/rejected.json',bad)

        if result['total']!=expected: raise RuntimeError('Timed out before expected unique event count')
        c.commit()
        print(json.dumps(result,indent=2))
        return result
    finally: c.close()

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--topic',required=True)
    ap.add_argument('--group',default='aitu-python-almas'); ap.add_argument('--expected',type=int,default=10)
    args=ap.parse_args(); consume(args.topic,args.group,args.expected)
