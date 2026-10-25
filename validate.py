"""Offline TOA5 comparison and collection coverage report."""
import argparse
import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from app import record_line


def report(reference, collected):
    def read(path):
        with Path(path).open(newline='',encoding='utf-8') as f:
            reader=csv.reader(f)
            headers=[next(reader) for _ in range(4)]
            rows=list(reader)
        return headers,rows
    rh,rr=read(reference)
    ch,cr=read(collected)
    old={(r[0],r[1]):r for r in rr if r}
    new={(r[0],r[1]):r for r in cr if r}
    reference_versions={}
    for row in rr:
        if row:
            reference_versions.setdefault((row[0],row[1]),set()).add(tuple(row))
    overlap=old.keys() & new.keys()
    ordered=sorted(new.values(),key=lambda r:(r[0],int(r[1])))
    deltas=[(datetime.fromisoformat(b[0])-datetime.fromisoformat(a[0])).total_seconds() for a,b in zip(ordered,ordered[1:])]
    interval=Counter(d for d in deltas if d>0).most_common(1)
    expected=interval[0][0] if interval else None
    record_gaps=[int(b[1])-int(a[1])-1 for a,b in zip(ordered,ordered[1:]) if int(b[1])>int(a[1])+1]
    differences=[{'timestamp':k[0],'record':k[1],'reference':old[k],'collected':new[k]} for k in sorted(overlap) if old[k]!=new[k]]
    return {'reference':str(reference),'collected':str(collected),'headers_equal':rh==ch,
            'reference_headers':rh,'collected_headers':ch,'records':len(cr),'unique_records':len(new),
            'reference_records':len(rr),'reference_duplicate_rows':len(rr)-len(old),
            'reference_conflicting_duplicate_keys':sum(len(v)>1 for v in reference_versions.values()),
            'collected_duplicate_rows':len(cr)-len(new),'reference_only_records':len(old.keys()-new.keys()),
            'collected_only_records':len(new.keys()-old.keys()),
            'first':ordered[0][0] if ordered else None,'last':ordered[-1][0] if ordered else None,
            'typical_interval_seconds':expected,'gaps':len(record_gaps),'missing_record_numbers':sum(record_gaps),
            'timestamp_gap_candidates':sum(d>expected for d in deltas) if expected else 0,
            'interval_note':'Timestamp gaps can reflect legitimate historical collection-interval changes; record-number gaps are reported separately.',
            'record_resets':sum(int(b[1])<int(a[1]) for a,b in zip(ordered,ordered[1:])),
            'overlap':len(overlap),'different_overlap_records':len(differences),'difference_examples':differences[:20]}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('reference')
    p.add_argument('collected')
    p.add_argument('--report',default='validation/comparison.json')
    a=p.parse_args()
    result=report(a.reference,a.collected)
    target=Path(a.report)
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))
