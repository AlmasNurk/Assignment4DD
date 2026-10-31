import csv, json, os, time, uuid, threading, statistics, platform
from datetime import datetime, timezone
from pathlib import Path
import psutil
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from producer import producer
from common import validate, Aggregator, save

BOOT=os.getenv('BOOTSTRAP_SERVERS','localhost:9092')
def create_topic(name):
    admin=KafkaAdminClient(bootstrap_servers=BOOT)
    try: admin.create_topics([NewTopic(name,num_partitions=3,replication_factor=1)])
    finally: admin.close()

def measure(rate,seconds=30):
    run=uuid.uuid4().hex[:10]; topic='aitu-almas-bench-'+run
    create_topic(topic)
    c=KafkaConsumer(topic,bootstrap_servers=BOOT,group_id='bench-'+run,
       auto_offset_reset='earliest',enable_auto_commit=False)

    until=time.monotonic()+60
    while not c.assignment():
        c.poll(timeout_ms=100)
        if time.monotonic()>until: raise RuntimeError('No partition assignment')
    p=producer(); n=rate*seconds; errors=[]; samples=[]; a=Aggregator()
    raw=[]; proc=psutil.Process(); cpu0=sum(proc.cpu_times()[:2]); t0=time.perf_counter()
    start_ns=time.time_ns()
    def send():
        try:
            futures=[]
            for i in range(n):
                delay=t0+i/rate-time.perf_counter()
                if delay>0: time.sleep(delay)
                e={'event_id':f'{run}-{i}','timestamp':datetime.now(timezone.utc).isoformat(),
                   'research_group':f'G{i%3+1:02}','job_type':['ml','image','text','simulation'][i%4],
                   'duration_sec':float(i%100+1),'status':'failed' if i%5==0 else 'completed',
                   'cpu_pct':float(30+i%70),'sent_ns':time.time_ns()}
                futures.append(p.send(topic,key=e['research_group'],value=e))
            p.flush(timeout=60)
            for f in futures: f.get(timeout=30)
        except Exception as exc: errors.append(repr(exc))
    thread=threading.Thread(target=send,daemon=True); thread.start()
    deadline=t0+seconds+90
    try:
        while len(samples)<n and time.perf_counter()<deadline:
            for records in c.poll(timeout_ms=100).values():
                for r in records:
                    e=json.loads(r.value); validate(e)
                    if not a.add(e): continue
                    elapsed=(time.time_ns()-e['sent_ns'])/1e6
                    samples.append(elapsed); raw.append([e['event_id'],e['sent_ns'],elapsed])
            if errors: raise RuntimeError(errors)
        thread.join(timeout=65)
        if thread.is_alive() or errors or len(samples)!=n:
            raise RuntimeError(f'Incomplete run: {len(samples)}/{n}; {errors}')
        elapsed=max(seconds,time.perf_counter()-t0)
        cpu=100*(sum(proc.cpu_times()[:2])-cpu0)/elapsed
        row={'input_rate':rate,'count':len(samples),'elapsed_sec':elapsed,
             'throughput_eps':len(samples)/elapsed,'avg_latency_ms':statistics.mean(samples),
             'p95_latency_ms':sorted(samples)[int(.95*(len(samples)-1))],
             'client_cpu_pct':cpu,'topic':topic,
             'bottleneck':'Not isolated; inspect latency, client CPU and broker stats'}
        with open(f'results/latency_{rate}.csv','w',newline='') as f:
            w=csv.writer(f); w.writerow(['event_id','sent_ns','latency_ms']); w.writerows(raw)
        save(f'results/bench_aggregates_{rate}.json',a.result())
        save(f'results/bench_{rate}.json',row)
        return row
    finally:
        c.close(); p.close(timeout=10)

def main():
    rows=[measure(rate) for rate in (10,100,500)]
    with open('results/performance.csv','w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
    save('results/environment.json',{'platform':platform.platform(),'python':platform.python_version(),
         'logical_cpus':psutil.cpu_count(),'ram_gb':psutil.virtual_memory().total/1e9,
         'time_utc':datetime.now(timezone.utc).isoformat(),'measurement_scope':'producer plus consumer Python process; excludes Kafka JVM'})
    print(json.dumps(rows,indent=2))
if __name__=='__main__': main()
