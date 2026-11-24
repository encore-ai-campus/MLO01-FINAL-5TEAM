"""Cumulative CF lifecycle: merge historical samples, append events, rebuild RDB."""
import argparse
import csv
import hashlib
import html
import json
import math
import os
import shutil
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from . import cf
from .core import iso,timestamp
from .pipeline import write_json
from .store import NeighborStore,checksum,code_version,install


def atomic_json(file,obj):
    file=Path(file);temp=file.with_suffix(file.suffix+'.new')
    write_json(temp,obj);os.replace(temp,file)


def merge(parts,output):
    output=Path(output).resolve();mf=output.with_suffix('.manifest.json')
    documents=[json.loads(Path(p).with_suffix('.manifest.json').read_text('utf-8')) for p in parts]
    if any(not d['complete'] for d in documents):raise ValueError('Only complete samples can be merged')
    if len({(d['modulus'],d['seed']) for d in documents})!=1:raise ValueError('All months must use the same user sample')
    inputs=[{'database':str(Path(p).resolve()),'manifest_sha256':checksum(Path(p).with_suffix('.manifest.json'))} for p in parts]
    if output.exists():
        old=json.loads(mf.read_text('utf-8'))
        if old.get('parts')==inputs and old['complete']:
            print('Reusing complete cumulative event store: '+str(output),flush=True);return
        raise ValueError('Another cumulative store exists; it was preserved')
    source_names=[Path(s['path']).name for d in documents for s in d['sources']]
    if len(set(source_names))!=len(source_names):raise ValueError('Monthly sources overlap')
    partial=output.with_suffix('.partial.sqlite')
    if partial.exists():raise ValueError('Partial merge exists; preserve it before retrying')
    output.parent.mkdir(parents=True,exist_ok=True)
    with closing(sqlite3.connect(parts[0])) as source,closing(sqlite3.connect(partial)) as dest:
        source.backup(dest)
        for part in parts[1:]:
            dest.execute('ATTACH DATABASE ? AS additional',(str(Path(part).resolve()),))
            dest.execute('INSERT INTO events SELECT * FROM additional.events');dest.commit()
            dest.execute('DETACH DATABASE additional')
        count=dest.execute('SELECT COUNT(*) FROM events').fetchone()[0]
        users=dest.execute('SELECT COUNT(DISTINCT user_id) FROM events').fetchone()[0]
        lo,hi=dest.execute('SELECT MIN(ts),MAX(ts) FROM events').fetchone()
    partial.rename(output)
    atomic_json(mf,{'complete':True,'parts':inputs,'sources':[s for d in documents for s in d['sources']],
                  'modulus':documents[0]['modulus'],'seed':documents[0]['seed'],
                  'sample_rows':count,'sample_users':users,'min_ts':lo,'max_ts':hi,
                  'ingested_batches':[],'purpose':'cumulative CF training and append-only real event history'})
    print(json.dumps({'database':str(output),'rows':count,'users':users,'start':iso(lo),'end':iso(hi)}),flush=True)


def ingest(database,events_file):
    """Append real platform JSONL events atomically; conflicting retries roll back."""
    database=Path(database);mf=database.with_suffix('.manifest.json')
    manifest=json.loads(mf.read_text('utf-8'))
    if not manifest['complete']:raise ValueError('Incomplete cumulative event store')
    events_file=Path(events_file);digest=checksum(events_file)
    if any(b['sha256']==digest for b in manifest.get('ingested_batches',[])):
        print(json.dumps({'reused_batch':digest,'inserted':0}),flush=True);return 0
    added=reused=0
    conn=sqlite3.connect(database)
    try:
        with conn:
            with events_file.open(encoding='utf-8') as stream:
                for row_no,line in enumerate(stream):
                    if not line.strip():continue
                    event=json.loads(line)
                    action={'view':0,'cart':1,'purchase':2}.get(event['event_type'])
                    if action is None:raise ValueError('event_type must be view, cart or purchase')
                    at=datetime.fromisoformat(event['event_time'].replace('Z','+00:00'))
                    if at.tzinfo is None:raise ValueError('event_time must include timezone')
                    ts=int(at.timestamp())
                    if ts<manifest['min_ts']:raise ValueError('Live event precedes the historical data start')
                    price=event.get('price')
                    if price is not None:
                        price=float(price)
                        if not math.isfinite(price) or price<0:raise ValueError('Invalid price')
                    event_id=event['event_id']
                    if not isinstance(event_id,str) or not event_id or len(event_id)>200:raise ValueError('A stable event_id is required')
                    logical=(ts,int(event['user_id']),int(event['product_id']),action,
                             str(event.get('category_code') or event.get('category_id') or 'unknown'),
                             str(event.get('brand') or 'unknown'),price,str(event.get('user_session') or ''))
                    previous=conn.execute('SELECT ts,user_id,item_id,action,category,brand,price,raw_session FROM events WHERE event_id=?',(event_id,)).fetchone()
                    if previous is not None:
                        if previous!=logical:raise ValueError('Conflicting event_id; the entire batch was rolled back')
                        reused+=1;continue
                    conn.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                        (event_id,ts,row_no,'live:'+digest[:16],*logical[1:]))
                    added+=1
            count=conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
            users=conn.execute('SELECT COUNT(DISTINCT user_id) FROM events').fetchone()[0]
            lo,hi=conn.execute('SELECT MIN(ts),MAX(ts) FROM events').fetchone()
        manifest.update(sample_rows=count,sample_users=users,min_ts=lo,max_ts=hi)
        manifest.setdefault('ingested_batches',[]).append({'sha256':digest,'inserted':added,'existing':reused,
                   'recorded_at':datetime.now(timezone.utc).isoformat(),'source':str(events_file.resolve())})
        manifest['sampling_note']='Historical monthly data is a fixed user sample. Post-bootstrap live events are appended as supplied without re-sampling.'
        atomic_json(mf,manifest)
    finally:conn.close()
    print(json.dumps({'inserted':added,'existing':reused,'total':count}),flush=True)
    return added


def build(database,config,output,cutoff=None,if_due=False):
    started=time.monotonic();database=Path(database).resolve();output=Path(output).resolve()
    c=json.loads(Path(config).read_text('utf-8'))
    mf=database.with_suffix('.manifest.json');data=json.loads(mf.read_text('utf-8'))
    if not data['complete']:raise ValueError('Incomplete event store')
    with closing(sqlite3.connect(f'file:{database.as_posix()}?mode=ro',uri=True)) as conn:
        count=conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
        maximum=conn.execute('SELECT MAX(ts) FROM events').fetchone()[0]
    if count!=data['sample_rows']:raise ValueError('Event manifest row count is inconsistent')
    end=timestamp(cutoff) if cutoff else maximum+1
    cf.training_bounds(end,c)
    output.mkdir(parents=True,exist_ok=True)
    if if_due and (output/'latest.json').exists():
        old=json.loads((output/'latest.json').read_text('utf-8'))
        if end<old['cutoff_ts']+c['cf_refresh_days']*86400:
            print(json.dumps({'skipped':'CF refresh not due','next_due':iso(old['cutoff_ts']+c['cf_refresh_days']*86400)}),flush=True);return
    key=hashlib.sha256((checksum(mf)+cf.cache_signature(c,c['cf_weights'])+str(end)+checksum(config)).encode()).hexdigest()[:24]
    job=output/'jobs'/f'{key}.json';job.parent.mkdir(parents=True,exist_ok=True)
    if job.exists():
        done=json.loads(job.read_text('utf-8'))
        if done['status']=='completed':
            install(done['bundle_path'],output/'serving.sqlite')
            print(json.dumps({'reused':True,**done}),flush=True);return
    lock=job.with_suffix('.lock');fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY);os.close(fd)
    try:
        atomic_json(job,{'status':'training','cutoff':iso(end)})
        print(f'Fitting {c["cf_history_mode"]} CF from {c.get("cf_history_start","all available history")} strictly before {iso(end)}; {count:,} stored events',flush=True)
        data_version=checksum(mf)
        neighbors,support=cf.fit_db(database,end,tuple(c['cf_weights']),c)
        if not support['relations']:raise ValueError('No supported CF relations')
        if checksum(mf)!=data_version:raise ValueError('Events changed while fitting; keep the completed model and rebuild against a stable cutoff')
        print('CF fitted: '+json.dumps(support),flush=True)
        version=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+f'-{c["cf_history_mode"]}-cf'
        bundle=output/'bundles'/version;bundle.mkdir(parents=True)
        write_json(bundle/'neighbors.json',neighbors)
        digest=checksum(bundle/'neighbors.json');retriever='cf-'+digest[:20]
        source=Path(json.loads(Path(c['ranker_comparison_source']).read_text('utf-8'))['bundle_path'])
        original=json.loads((source/'manifest.json').read_text('utf-8'))
        for name in ['ranker.txt','feature_schema.json']:
            if checksum(source/name)!=original['checksums'][name]:raise ValueError('Comparison model artifact mismatch')
            shutil.copyfile(source/name,bundle/name)
        old_policy=json.loads((source/'policy.json').read_text('utf-8'))
        policy={**old_policy,**c,'cf_cutoff':iso(end),'cf_cutoff_ts':end,'ranker_comparison_only':True}
        write_json(bundle/'policy.json',policy)
        promotion={'accepted':False,'decision':'new_retriever_requires_fresh_evaluation',
                   'note':f'{c["cf_history_mode"]} CF trained; existing LightGBM copied only for local comparison. No accuracy claim or auto-activation.'}
        report={'training':support,'cutoff':iso(end),'data':data,'data_version':data_version,
                'cf_policy':c,'ranker_retrained':False,'comparison_ranker_sha256':checksum(bundle/'ranker.txt'),
                'promotion':promotion,'runtime_seconds_before_RDB':round(time.monotonic()-started,2),
                'next_scheduled_cutoff':iso(end+c['cf_refresh_days']*86400),
                'accuracy_metrics':None,'test_playground_actions_ingested':False}
        write_json(bundle/'training.json',report)
        with (bundle/'item_neighbors.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.writer(f);writer.writerow(['retriever_version','source_item_id','neighbor_item_id','similarity','neighbor_rank'])
            for p,values in neighbors.items():writer.writerows((retriever,p,n,s,i+1) for i,(n,s) in enumerate(values))
        files=['neighbors.json','ranker.txt','feature_schema.json','policy.json','training.json','item_neighbors.csv']
        manifest={'bundle_version':version,'retriever_version':retriever,'ranker_version':original['ranker_version'],
                  'retriever_snapshots':[{'cutoff_ts':end,'cutoff':iso(end),'file':'neighbors.json','version':retriever,'sha256':digest,'support':support}],
                  'feature_schema_version':1,'session_policy_version':f'gap-{c["session_gap_seconds"]}-v1',
                  'data_version':data_version,'ranker_training_data_version':original['data_version'],
                  'code_version':code_version(),'promotion':promotion,
                  'checksums':{name:checksum(bundle/name) for name in files}}
        write_json(bundle/'manifest.json',manifest)
        database_out=output/'serving.sqlite';install(bundle,database_out)
        store=NeighborStore(database_out,retriever)
        try:
            sample=list(neighbors)[:30]
            if store.get_many(sample)!={p:neighbors[p] for p in sample}:raise ValueError('RDB and cumulative relation file differ')
        finally:store.close()
        latest={'bundle_path':str(bundle),'database':str(database_out),'events_database':str(database),
                'cutoff_ts':end,'activated':False,'promotion':promotion}
        atomic_json(output/'latest.json',latest);write_json(output/'report.json',report)
        range_label=f'최근 {c["cf_lookback_days"]}일' if c['cf_history_mode']=='rolling' else '기준 시점 이전 보유 기간 전체'
        (output/'report.html').write_text('<!doctype html><html lang="ko"><meta charset="utf-8"><title>'+html.escape(range_label)+' CF 학습</title><style>body{font:16px/1.7 system-ui;max-width:1000px;margin:40px auto;padding:20px}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f3f5f1;padding:20px}</style><h1>'+html.escape(range_label)+' CF 학습</h1><p>CF 학습 기간은 '+html.escape(range_label)+'입니다. 원본 이벤트 저장소에는 이전 기간의 행동도 보존합니다. 사용자 표본과 자원 상한은 학습 기간 제한과 별개입니다. LightGBM을 재학습하거나 새 성능을 검증한 결과가 아니며 자동 활성화하지 않았습니다.</p><pre>'+html.escape(json.dumps(report,ensure_ascii=False,indent=2))+'</pre></html>','utf-8')
        atomic_json(job,{'status':'completed','bundle_path':str(bundle),'runtime_seconds':round(time.monotonic()-started,2)})
        print(json.dumps({'completed':True,'latest':latest,'training':support,'runtime_seconds':round(time.monotonic()-started,2)},indent=2),flush=True)
    except Exception as e:
        atomic_json(job,{'status':'failed','error':str(e)});raise
    finally:lock.unlink()


if __name__=='__main__':
    p=argparse.ArgumentParser(description='Cumulative CF and append-only user events')
    sub=p.add_subparsers(dest='command',required=True)
    m=sub.add_parser('merge');m.add_argument('parts',nargs='+');m.add_argument('--output',default='data/events-all-2pct.sqlite')
    i=sub.add_parser('ingest');i.add_argument('--events',default='data/events-all-2pct.sqlite');i.add_argument('--input',required=True)
    b=sub.add_parser('build');b.add_argument('--events',default='data/events-all-2pct.sqlite');b.add_argument('--config',default='config-rolling30.json');b.add_argument('--output',default='artifacts-rolling30');b.add_argument('--cutoff');b.add_argument('--if-due',action='store_true')
    a=p.parse_args()
    if a.command=='merge':merge(a.parts,a.output)
    elif a.command=='ingest':ingest(a.events,a.input)
    else:build(a.events,a.config,a.output,a.cutoff,a.if_due)
