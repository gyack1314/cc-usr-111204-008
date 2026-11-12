"""清点压缩清单行与对象样本，不选择对象版本。"""
import csv
import gzip
import io
from .io import asset,unique

def summarize(v,root):
    unique(v['shards'],'key');sizes=[]
    for s in v['shards']:
        raw,digest=asset(root,s['file']);text=gzip.decompress(raw).decode('utf8');rows=list(csv.reader(io.StringIO(text)))
        if any(len(r)!=len(v['schema']) for r in rows):raise ValueError('清单列数不符')
        sizes.append({'key':s['key'],'csv_rows':len(rows),'gzip_bytes':len(raw),'sha256':digest})
    return {'generation':v['generation'],'shards':sizes,'object_examples':len(v['objects'])}
