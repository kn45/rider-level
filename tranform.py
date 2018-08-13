from __future__ import print_function
import sys

data = {}
for ln in sys.stdin:
    flds = ln.rstrip('\n').split('\t')
    if flds[0] not in ['320600', '360100']:
        continue
    key = (flds[0], flds[1], flds[2][:6])
    if key not in data:
        data[key] = [0.0] * 5
    for i, val in enumerate(flds[3:]):
        data[key][i] += float(val)

for key in data:
    cid, rid, m = key
    print(cid, rid, m, *data[key], sep='\t')
