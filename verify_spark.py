import json
from pathlib import Path
from collections import defaultdict
expected=defaultdict(list)
for e in json.loads(Path('data/events.json').read_text()):
    second=int(e['timestamp'][17:19]); key=(second//5*5,e['job_type'])
    expected[key].append(e['duration_sec'])
actual={}
for p in sorted(Path('results/spark').glob('batch_*/*.json'),key=lambda p:int(p.parent.name.split('_')[1])):
    for line in p.read_text().splitlines():
        r=json.loads(line); key=(int(r['window']['start'][17:19]),r['job_type']); actual[key]=r
assert set(actual)==set(expected),(actual,expected)
for k,values in expected.items():
    assert actual[k]['count']==len(values)
    assert abs(actual[k]['avg_duration']-sum(values)/len(values))<1e-8
print('PASS: all 9 Spark window and job_type groups agree with reference calculation')
