import json, math
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

REQUIRED = {'event_id','timestamp','research_group','job_type','duration_sec','status','cpu_pct'}
def validate(e):
    if not isinstance(e, dict) or not REQUIRED <= e.keys():
        raise ValueError('Missing required schema fields')
    for field in ('event_id','research_group'):
        if not isinstance(e[field],str) or not e[field].strip():
            raise ValueError('Empty or invalid '+field)
    if e['job_type'] not in ('simulation','ml','image','text'):
        raise ValueError('Invalid job_type')
    if e['status'] not in ('completed','failed'):
        raise ValueError('Invalid status')
    for field in ('duration_sec','cpu_pct'):
        if type(e[field]) not in (int,float) or not math.isfinite(e[field]):
            raise ValueError('Invalid numeric '+field)
    if e['duration_sec'] < 0 or not 0 <= e['cpu_pct'] <= 100:
        raise ValueError('Out of range numeric value')
    t=datetime.fromisoformat(e['timestamp'])
    if t.tzinfo is None: raise ValueError('timestamp requires timezone')
    return e

class Aggregator:

    def __init__(self):
        self.seen=set(); self.events=[]; self.duplicates=0
    def add(self,e):
        validate(e)
        if e['event_id'] in self.seen:
            self.duplicates+=1; return False
        self.seen.add(e['event_id']); self.events.append(e); return True
    def result(self):
        jobs=defaultdict(list); groups=defaultdict(list); windows=defaultdict(list)
        for e in self.events:
            jobs[e['job_type']].append(e); groups[e['research_group']].append(e)
            t=datetime.fromisoformat(e['timestamp'])
            start=t.replace(second=t.second//5*5,microsecond=0)
            windows[start].append(e)
        mean=lambda es,k: sum(e[k] for e in es)/len(es)
        return {'total':len(self.events), 'completed':sum(e['status']=='completed' for e in self.events),
                'failed':sum(e['status']=='failed' for e in self.events),'duplicates':self.duplicates,
                'jobs':[{ 'job_type':j,'completed':sum(e['status']=='completed' for e in es),
                         'failed':sum(e['status']=='failed' for e in es),'avg_duration':mean(es,'duration_sec')} for j,es in sorted(jobs.items())],
                'groups':[{ 'research_group':g,'avg_cpu':mean(es,'cpu_pct')} for g,es in sorted(groups.items())],
                'windows':[{ 'start':s.isoformat(),'end':(s+timedelta(seconds=5)).isoformat(),
                            'events':sorted(e['event_id'] for e in es),'count':len(es),
                            'failure_rate':sum(e['status']=='failed' for e in es)/len(es),
                            'avg_cpu':mean(es,'cpu_pct')} for s,es in sorted(windows.items())]}

def save(path,obj):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    Path(path).write_text(json.dumps(obj,indent=2),encoding='utf-8')

if __name__=='__main__':
    a=Aggregator()
    for e in json.loads(Path('data/events.json').read_text()): a.add(e)
    save('results/offline_results.json',a.result())
    print(json.dumps(a.result(),indent=2))
