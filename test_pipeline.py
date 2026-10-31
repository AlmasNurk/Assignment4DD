import unittest, json, copy
from pathlib import Path
from common import Aggregator, validate
class Tests(unittest.TestCase):
    def setUp(self): self.es=json.loads(Path('data/events.json').read_text())
    def result(self,events):
        a=Aggregator()
        for e in events:a.add(e)
        return a.result()
    def test_counts_means_boundaries(self):
        r=self.result(self.es)
        self.assertEqual((r['total'],r['completed'],r['failed']),(10,8,2))
        self.assertEqual([w['count'] for w in r['windows']],[4,5,1])
        self.assertEqual([w['avg_cpu'] for w in r['windows']],[66.25,75.2,78])
        self.assertEqual([w['failure_rate'] for w in r['windows']],[.25,.2,0])
        self.assertAlmostEqual(next(j['avg_duration'] for j in r['jobs'] if j['job_type']=='ml'),157/3)
        self.assertAlmostEqual(r['groups'][0]['avg_cpu'],68.5)
    def test_out_of_order(self): self.assertEqual(self.result(self.es),self.result(self.es[::-1]))
    def test_duplicate(self):
        r=self.result(self.es+self.es[:1]); self.assertEqual(r['total'],10); self.assertEqual(r['duplicates'],1)
    def test_invalid(self):
        for field,value in [('cpu_pct',101),('cpu_pct',float('nan')),('duration_sec',-1),('duration_sec',True),('status','done'),('timestamp','2026-10-31T14:00:00')]:
            e=copy.deepcopy(self.es[0]); e[field]=value
            with self.assertRaises((ValueError,TypeError)): validate(e)
        e=copy.deepcopy(self.es[0]); del e['event_id']
        with self.assertRaises(ValueError):validate(e)
if __name__=='__main__':unittest.main()
