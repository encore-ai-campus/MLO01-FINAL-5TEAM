"""Paired April replay: cumulative versus 30-day CF, weekly past-only fits."""
import argparse
import gc
import hashlib
import html
import json
import os
import sqlite3
import time
from collections import defaultdict
from contextlib import closing
from datetime import datetime,timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np

from audit_2pct import recent_scores
from . import cf
from .core import Event,State,generate,iso,session_for,targets,timestamp
from .pipeline import BOOTSTRAP_UNIT,evaluate,improvement_ci,predict_all,query_plan,write_json
from .store import checksum,code_version
from .temporal_cf import cutoff_for_time


ROOT=Path(__file__).resolve().parents[1]
START='2020-04-01T00:00:00Z'
END='2020-05-01T00:00:00Z'


def assign_sessions(events,gap,boundary='at_least'):
    sessions=defaultdict(list);last={}
    for order,e in enumerate(events):
        previous=last.get(e.user)
        e.session=session_for(e,previous,gap,boundary)
        e.order=order;sessions[e.session].append(e);last[e.user]=e
    return dict(sessions)


def read_month(database,start,end,gap,boundary='at_least'):
    with closing(sqlite3.connect(f'file:{Path(database).resolve().as_posix()}?mode=ro',uri=True)) as c:
        # Prefix catches visits crossing the month boundary; they are excluded.
        events=[Event(*row) for row in c.execute('SELECT * FROM events WHERE ts>=? AND ts<? ORDER BY ts,source,row_no',(start-gap,end))]
    return events,assign_sessions(events,gap,boundary)


class PastOnlyCF:
    def __init__(self,database,config,cache):
        self.database=database;self.c=config
        self.cache=Path(cache)/cf.cache_signature(config,config['cf_weights'])[:24]
        self.cache.mkdir(parents=True,exist_ok=True)
        self.current_cutoff=None;self.current=None;self.snapshots={}

    def for_time(self,as_of):
        cutoff=cutoff_for_time(as_of,self.c['cf_cutoff_ts'],self.c['cf_refresh_days'])
        if cutoff>as_of:raise ValueError('Future CF snapshot selected')
        if cutoff==self.current_cutoff:return self.current
        file=self.cache/f'{cutoff}.json';meta=file.with_suffix('.meta.json')
        if file.exists() and meta.exists():
            info=json.loads(meta.read_text('utf-8'))
            if checksum(file)!=info['sha256']:raise ValueError('Evaluation snapshot checksum mismatch')
            values={int(p):rows for p,rows in json.loads(file.read_text('utf-8')).items()}
        else:
            print(f'Fitting {self.c["cf_history_mode"]} snapshot before {iso(cutoff)}',flush=True)
            tick=time.monotonic()
            values,support=cf.fit_db(self.database,cutoff,tuple(self.c['cf_weights']),self.c)
            if support['newest_event_ts'] is not None and support['newest_event_ts']>=cutoff:
                raise ValueError('Snapshot training used future events')
            file.write_text(json.dumps(values,separators=(',',':')),'utf-8')
            info={'cutoff_ts':cutoff,'cutoff':iso(cutoff),'sha256':checksum(file),'support':support,
                  'fit_seconds':round(time.monotonic()-tick,2)}
            write_json(meta,info)
            print(f'Snapshot ready: {self.c["cf_history_mode"]}, events={support["events_considered"]:,}, items={support["items"]:,}, {info["fit_seconds"]}s',flush=True)
        support=info['support']
        if info['cutoff_ts']!=cutoff or (support['newest_event_ts'] is not None and support['newest_event_ts']>=cutoff):
            raise ValueError('Invalid historical snapshot boundary')
        self.snapshots[cutoff]={**info,'file':str(file.resolve())}
        self.current_cutoff,self.current=cutoff,values
        return values


def replay_month(database,events,sessions,plan,timeline,config,*,state_factory=State,query_features=None,
                 candidate_builder=generate):
    """Stream historical context, retaining month labels and bounded user state."""
    state=state_factory(config);month={e.event_id:e for e in events}
    selected={events[order].event_id for order in plan}
    last_request=max(events[order].ts for order in plan)
    queries=[];previous={}
    with closing(sqlite3.connect(f'file:{Path(database).resolve().as_posix()}?mode=ro',uri=True)) as conn:
        for global_order,row in enumerate(conn.execute('SELECT * FROM events WHERE ts<=? ORDER BY ts,source,row_no',(last_request,))):
            # Reuse month objects: labels and requests must share the same order.
            e=month.get(row[0])
            if e is None:
                e=Event(*row);prev=previous.get(e.user)
                e.session=session_for(e,prev,config['session_gap_seconds'],config.get('session_gap_boundary','at_least'))
                e.order=global_order
            previous[e.user]=e
            state.observe(e)
            if e.event_id not in selected:continue
            relations=timeline.for_time(e.ts)
            generated=candidate_builder(state,e,relations,tuple(config['request_weights']))
            if query_features is not None:query_features(state,e,generated)
            truth,exclusions=targets(sessions[e.session],e,state.metadata.keys(),generated['excluded'])
            generated.pop('evidence')
            queries.append({**generated,'truth':truth,'exclusions':exclusions,
                'user':e.user,'request_id':hashlib.sha256(e.event_id.encode()).hexdigest()[:20],
                'session_id':e.session,'as_of':e.ts,'current_item_id':e.item,
                'first_request':e.event_id==next(x.event_id for x in sessions[e.session] if x.action<2),
                'user_history_events':len(state.history[e.user])})
            if len(queries)%1000==0:print(f'{config["cf_history_mode"]}: evaluated inputs {len(queries):,}/{len(plan):,}, at {iso(e.ts)}',flush=True)
    if len(queries)!=len(plan):raise ValueError('Selected month requests were lost during streaming replay')
    return queries


def paired_ci(a,b,field,protocol):
    # Existing bootstrap groups requests by user; map the chosen metric into it.
    aa=[{**r,'ndcg':r[field]} for r in a]
    bb=[{**r,'ndcg':r[field]} for r in b]
    return improvement_ci(aa,bb,protocol['bootstrap_repeats'],protocol['seed'],
                          unit=protocol.get('bootstrap_unit', BOOTSTRAP_UNIT))


def assert_common_popularity(a,b):
    keys=['request_id','user','has_truth','hit','recall','ndcg']
    if len(a)!=len(b) or any(any(left[k]!=right[k] for k in keys) for left,right in zip(a,b)):
        raise ValueError('Evaluation populations or common popularity baseline differ')


def summarize(queries,booster,config,protocol):
    predictions=predict_all(booster,queries,config['threads'])
    recent=[recent_scores(q) for q in queries]
    outputs={};all_rows={}
    for name,indexes in [('all',list(range(len(queries)))),
        ('first_request',[i for i,q in enumerate(queries) if q['first_request']]),
        ('later_request',[i for i,q in enumerate(queries) if not q['first_request']])]:
        subset=[queries[i] for i in indexes];metrics={};rows={}
        for mode in ['popular','cf','recent_interest','ranker']:
            scores=[predictions[i] for i in indexes] if mode=='ranker' else [recent[i] for i in indexes] if mode=='recent_interest' else None
            metrics[mode],rows[mode]=evaluate(subset,'ranker' if mode=='recent_interest' else mode,scores)
        outputs[name]={'metrics':metrics,'ranker_vs_recent':paired_ci(rows['ranker'],rows['recent_interest'],'ndcg',protocol)}
        if name=='all':all_rows=rows
    return outputs,all_rows


def write_report(file,report):
    rows=''
    for arm in ['rolling30','cumulative']:
        for mode,m in report['arms'][arm]['groups']['all']['metrics'].items():
            fmt=lambda x:'N/A' if x is None else f'{x:.4f}'
            values=[arm,mode,m['queries_all_mature'],m['queries_with_eligible_future_purchase'],
                    fmt(m['purchase_candidate_recall_at_100']),fmt(m['purchase_hit_rate_at_10']),fmt(m['purchase_ndcg_at_10'])]
            rows+='<tr>'+''.join('<td>'+html.escape(str(x))+'</td>' for x in values)+'</tr>'
    page='''<!doctype html><html lang="ko"><meta charset="utf-8"><title>누적 CF 대 최근 30일 CF 평가</title><style>body{font:16px/1.7 system-ui;max-width:1200px;margin:35px auto;padding:20px;color:#253b2e;background:#f5f7f1}table{width:100%;border-collapse:collapse;background:white}td,th{padding:10px;border-bottom:1px solid #ddd;text-align:left}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:white;padding:20px}</style><h1>4월 시간순 재현: 누적 CF와 최근 30일 CF</h1><p>동일한 4월 요청에 대해 4월 1·8·15·22·29일 경계 직전까지만 학습한 관계표를 사용합니다. 4월 말까지 학습한 최신 모델을 4월 초에 미리 사용하지 않습니다. 사용자 표본·상품 상한·행동 가중치·제외 규칙·정렬 모델을 동일하게 고정했습니다.</p><p>지표는 추천 정책상 미래 구매 정답이 있는 요청에 조건부이며 구매 전환율이 아닙니다. 기본 정렬은 장바구니·최근 관심순이고 LightGBM은 기존 저장 모델의 비교 결과입니다. 새 순위 모델을 튜닝하거나 자동 배포하지 않았습니다.</p>'''
    page+='<h2>비교 결과</h2><table><tr><th>CF 기간</th><th>정렬</th><th>전체 요청</th><th>구매 정답 요청</th><th>후보 회수율@100</th><th>적중률@10</th><th>NDCG@10</th></tr>'+rows+'</table>'
    page+='<h2>누적−30일 차이와 사용자 단위 95% 구간</h2><pre>'+html.escape(json.dumps(report['comparisons'],ensure_ascii=False,indent=2))+'</pre>'
    page+='<h2>평가 범위와 해석</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in report['limitations'])+'</ul>'
    page+='<p><a href="report.json">상황별 수치와 학습 시점 전체 기록</a> · <a href="protocol.json">결과 전 고정한 평가 절차</a></p></html>'
    file.write_text(page,'utf-8')


def run(database,output,limit=16000):
    if not 1<=limit<=100000:raise ValueError('Query limit must be 1..100000')
    database=Path(database).resolve();output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    c=json.loads((ROOT/'config-cumulative.json').read_text('utf-8'))
    source=Path(json.loads((ROOT/c['ranker_comparison_source']).read_text('utf-8'))['bundle_path'])
    m=json.loads((source/'manifest.json').read_text('utf-8'))
    if checksum(source/'ranker.txt')!=m['checksums']['ranker.txt']:raise ValueError('Comparison ranker checksum mismatch')
    mf=database.with_suffix('.manifest.json')
    protocol={'created_at':datetime.now(timezone.utc).isoformat(),'window':[START,END],
        'data_manifest_sha256':checksum(mf),'query_limit':limit,'seed':c['seed'],
        'queries_per_session':3,'query_min_interval_seconds':60,
        'refresh_anchor':START,'refresh_days':7,'arms':{'rolling30':{'mode':'rolling','days':30},'cumulative':{'mode':'cumulative','start':c['cf_history_start']}},
        'bootstrap_repeats':1000,'bootstrap_unit':BOOTSTRAP_UNIT,
        'primary_metric':'purchase_candidate_recall_at_100',
        'secondary_metrics':['purchase_hit_rate_at_10','purchase_ndcg_at_10'],
        'ranker_source':str(source),'ranker_sha256':checksum(source/'ranker.txt'),
        'ranker_retrained':False,'base_config':c,'code_version':code_version(),
        'baseline_source_sha256':checksum(ROOT/'audit_2pct.py'),
        'claim':'paired time-safe historical replay; April was previously included in a latest CF build, so this is not a claim of a wholly untouched blind dataset',
        'activate':False}
    protocol_file=output/'protocol.json'
    if protocol_file.exists():
        old=json.loads(protocol_file.read_text('utf-8'))
        if old.get('bootstrap_unit', BOOTSTRAP_UNIT) != protocol['bootstrap_unit']:
            raise ValueError('Frozen bootstrap unit changed; choose a separate output')
        for key in ['data_manifest_sha256','query_limit','code_version','ranker_sha256','baseline_source_sha256','base_config']:
            if old[key]!=protocol[key]:raise ValueError('Frozen evaluation changed; choose a separate output')
        protocol=old
    else:write_json(protocol_file,protocol)
    status=output/'status.json';lock=output/'evaluation.lock'
    if status.exists() and json.loads(status.read_text('utf-8')).get('status')=='completed':
        print(json.dumps({'reused':True,'report':str(output/'report.html')}),flush=True);return
    fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY);os.close(fd)
    started=time.monotonic()
    try:
        write_json(status,{'status':'running','protocol_sha256':checksum(protocol_file)})
        start,end=map(timestamp,[START,END])
        c={**c,'cf_cutoff_ts':start,'cf_cutoff':START,'future':[START,END],
           'rank_train':['2019-10-08T00:00:00Z','2019-10-20T00:00:00Z'],
           'validation':['2019-10-21T00:00:00Z','2019-10-25T00:00:00Z'],
           'test':['2019-10-27T00:00:00Z','2019-10-31T00:00:00Z'],
           'queries_per_session':3,'query_min_interval_seconds':60,
           'query_limits':{'rank_train':0,'validation':0,'test':0,'future':limit}}
        events,sessions=read_month(database,start,end,c['session_gap_seconds'])
        plan,stats=query_plan(events,sessions,c)
        plan={order:split for order,split in plan.items() if split=='future'}
        print(f'April month labels: {len(events):,} events, {len(sessions):,} sessions; evaluating {len(plan):,} identical requests',flush=True)
        booster=lgb.Booster(model_file=str(source/'ranker.txt'))
        arms={};retained={}
        cache=output/'snapshots'/protocol['data_manifest_sha256'][:24]
        for arm,mode,days in [('rolling30','rolling',30),('cumulative','cumulative',None)]:
            print('Starting '+arm,flush=True);tick=time.monotonic()
            policy={**c,'cf_history_mode':mode,'cf_lookback_days':days}
            timeline=PastOnlyCF(database,policy,cache)
            queries=replay_month(database,events,sessions,plan,timeline,policy)
            groups,rows=summarize(queries,booster,policy,protocol)
            arms[arm]={'groups':groups,'snapshots':list(timeline.snapshots.values()),'runtime_seconds':round(time.monotonic()-tick,2)}
            retained[arm]=rows
            with (output/(arm+'-request-metrics.jsonl')).open('w',encoding='utf-8') as f:
                for ranking,values in rows.items():
                    for value in values:f.write(json.dumps({'ranking':ranking,**value})+'\n')
            write_json(output/(arm+'.json'),arms[arm])
            print(json.dumps({'completed_arm':arm,'metrics':groups['all']['metrics']},indent=2),flush=True)
            del queries,timeline;gc.collect()
        # Outcomes, query IDs and popularity are independent of the CF history choice.
        a,b=retained['cumulative'],retained['rolling30']
        assert_common_popularity(a['popular'],b['popular'])
        comparisons={'candidate_recall_at_100':paired_ci(a['cf'],b['cf'],'candidate_recall',protocol),
            'recent_interest_ndcg_at_10':paired_ci(a['recent_interest'],b['recent_interest'],'ndcg',protocol),
            'recent_interest_hit_at_10':paired_ci(a['recent_interest'],b['recent_interest'],'hit',protocol),
            'ranker_ndcg_at_10':paired_ci(a['ranker'],b['ranker'],'ndcg',protocol)}
        if (checksum(mf)!=protocol['data_manifest_sha256'] or code_version()!=protocol['code_version']
            or checksum(source/'ranker.txt')!=protocol['ranker_sha256']
            or checksum(ROOT/'audit_2pct.py')!=protocol['baseline_source_sha256']):
            raise ValueError('Input or evaluation code changed during replay')
        report={'protocol':protocol,'query_plan':stats['future'],'arms':arms,'comparisons':comparisons,
            'runtime_seconds':round(time.monotonic()-started,2),'activated':False,
            'latest_May_model_used_for_April':False,'limitations':[
                'April only, fixed 2% user sample; 200 recent distinct items per user and 20,000 CF-supported products remain capped.',
                'Every weekly CF snapshot excludes events at or after its cutoff. The May 1 latest model was not used for past April requests.',
                'Both arms use identical requests, temporal context, exclusions and unchanged October-trained LightGBM; no ranking model is retrained here.',
                'Previously processed months are not described as a wholly untouched blind dataset; this is a paired historical replay.',
                'Previously viewed/carted products may return. First versus later request results are reported separately.',
                'Raw category inconsistencies found after November were not silently corrected; CF fitting uses user/product IDs and actions, while popularity/ranking metadata can still be affected.',
                'Offline purchase metrics do not establish conversion uplift, causal recommendation impact or current live-service accuracy.',
                'All results remain evaluation-only; no model or test-shop pointer is automatically switched.'
            ]}
        write_json(output/'report.json',report);write_report(output/'report.html',report)
        write_json(status,{'status':'completed','protocol_sha256':checksum(protocol_file),'runtime_seconds':report['runtime_seconds']})
        print(json.dumps({'completed':True,'comparisons':comparisons,'report':str(output/'report.html')},indent=2),flush=True)
    except Exception as e:
        write_json(status,{'status':'failed','error':str(e)});raise
    finally:lock.unlink()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--events',default='data/events-all-2pct.sqlite');p.add_argument('--output',default='artifacts-cumulative-eval');p.add_argument('--queries',type=int,default=16000)
    a=p.parse_args();run(a.events,a.output,a.queries)
