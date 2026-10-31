import subprocess, sys, uuid, json
from pathlib import Path
from benchmark import create_topic
from common import save

Path('results').mkdir(exist_ok=True)

if Path('results/consumer_results.json').exists():
    raise SystemExit('Results already exist. Rename results to results_previous before another run.')
topic='aitu-research-almas-'+uuid.uuid4().hex[:8]
create_topic(topic)
save('results/run_config.json',{'topic':topic,'partitions':3,'replication_factor':1,
    'consumer_group':'aitu-python-almas'})
def run(name,cmd):
    print('Running',name,flush=True)
    with open('results/'+name+'.log','w') as f:
        subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True)
run('tests',[sys.executable,'-m','unittest','-v','test_pipeline'])
run('producer',[sys.executable,'producer.py','--topic',topic])
run('consumer',[sys.executable,'consumer.py','--topic',topic])
actual=json.loads(Path('results/consumer_results.json').read_text())
from common import Aggregator
a=Aggregator()
for e in json.loads(Path('data/events.json').read_text()): a.add(e)
assert actual==a.result(), 'Kafka output differs from source calculations'
run('spark',['spark-submit','--packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3',
             'spark_stream.py','--topic',topic])
run('verify_spark',[sys.executable,'verify_spark.py'])
run('benchmark',[sys.executable,'benchmark.py'])
print('DONE: execution evidence is in results/',flush=True)
