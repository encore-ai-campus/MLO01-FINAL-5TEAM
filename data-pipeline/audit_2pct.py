"""Audit the selected development model against a simple recent-interest rule.

This never retrains or changes model selection. December remains untouched.
"""
import hashlib
import html
import json
from pathlib import Path

import reco
import lightgbm as lgb
import numpy as np
from reco.core import FEATURES, read_events
from reco.pipeline import evaluate, improvement_ci, predict_all, query_plan, replay, write_json
from reco.serve import select_retriever
from reco.store import checksum


class SavedSnapshots:
    def __init__(self, bundle, manifest, config):
        self.bundle, self.manifest, self.config = bundle, manifest, config
        self.version, self.values = None, None

    def for_time(self, as_of):
        version, _ = select_retriever(self.manifest, self.config, as_of)
        if version != self.version:
            snap = next(s for s in self.manifest['retriever_snapshots'] if s['version']==version)
            file = self.bundle/snap['file']
            if checksum(file) != self.manifest['checksums'][snap['file']]:
                raise ValueError('Saved relation checksum mismatch')
            self.values = {int(p):values for p,values in json.loads(file.read_text('utf-8')).items()}
            self.version = version
        return self.values


def recent_scores(q):
    """Carted items, then latest observed interest, then purchase popularity."""
    cart, gap, popularity = [FEATURES.index(n) for n in
                            ['session_carted','last_item_gap_log','purchase_popularity_log']]
    order = sorted(range(len(q['items'])), key=lambda i: (
        -q['features'][i,cart], q['features'][i,gap]<0,
        q['features'][i,gap] if q['features'][i,gap]>=0 else 1e9,
        -q['features'][i,popularity], q['items'][i]))
    result = np.zeros(len(order),dtype=np.float32)
    for rank,index in enumerate(order):
        result[index] = len(order)-rank
    return result


def run():
    root = Path(__file__).resolve().parent
    output = root/'artifacts-2pct'
    comparison = json.loads((output/'report.json').read_text('utf-8'))
    selected = comparison['decision']['selected_development_candidate']
    bundle = Path(next(r['bundle_path'] for r in comparison['runs'] if r['name']==selected))
    manifest = json.loads((bundle/'manifest.json').read_text('utf-8'))
    c = json.loads((bundle/'policy.json').read_text('utf-8'))
    events,sessions = read_events(root/'data/events-2pct.sqlite',c['session_gap_seconds'])
    plan,_ = query_plan(events,sessions,c)
    plan = {order:split for order,split in plan.items() if split=='future'}
    first_ids = {hashlib.sha256(next(e for e in seq if e.action<2).event_id.encode()).hexdigest()[:20]
                 for seq in sessions.values() if any(e.action<2 for e in seq)}
    print(f'Auditing {len(plan):,} identical November requests against recent-interest baseline',flush=True)
    queries = replay(events,sessions,plan,SavedSnapshots(bundle,manifest,c),
                     tuple(c['request_weights']),c,{'future'})['future']
    booster = lgb.Booster(model_file=str(bundle/'ranker.txt'))
    predictions = predict_all(booster,queries,c['threads'])
    recent = [recent_scores(q) for q in queries]
    groups = {}
    for name, indexes in [('all',range(len(queries))),
                          ('first_request',[i for i,q in enumerate(queries) if q['request_id'] in first_ids]),
                          ('later_request',[i for i,q in enumerate(queries) if q['request_id'] not in first_ids])]:
        indexes = list(indexes)
        subset = [queries[i] for i in indexes]
        metrics,rows = {},{}
        for mode in ['popular','cf','recent_interest','ranker']:
            scores = [predictions[i] for i in indexes] if mode=='ranker' else [recent[i] for i in indexes] if mode=='recent_interest' else None
            metrics[mode],rows[mode] = evaluate(subset,'ranker' if mode=='recent_interest' else mode,scores)
        groups[name] = {'metrics':metrics,'ranker_vs_recent_interest':improvement_ci(
            rows['ranker'],rows['recent_interest'],c['bootstrap_repeats'],c['seed'])}
    original = json.loads((bundle/'evaluation.json').read_text('utf-8'))['metrics']['future']['ranker']
    for metric in ['purchase_candidate_recall_at_100','purchase_hit_rate_at_10','purchase_ndcg_at_10']:
        if not np.isclose(groups['all']['metrics']['ranker'][metric],original[metric],rtol=0,atol=1e-12):
            raise ValueError('Saved-model replay did not reproduce the original evaluation: '+metric)
    report = {'bundle_path':str(bundle),'ranker_sha256':checksum(bundle/'ranker.txt'),
              'audit_source_sha256':checksum(__file__),'groups':groups,
              'note':'Development-only audit; no retraining or December data. Later requests can recommend previously viewed/carted products. Recent-interest rule uses the same candidates and exclusions.'}
    write_json(output/'audit.json',report)
    rows = ''
    for stage,result in groups.items():
        for mode,m in result['metrics'].items():
            values = [stage,mode,m['queries_all_mature'],m['queries_with_eligible_future_purchase'],
                      f"{m['purchase_hit_rate_at_10']:.4f}",f"{m['purchase_ndcg_at_10']:.4f}"]
            rows += '<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in values)+'</tr>'
    page = '<!doctype html><html lang="ko"><meta charset="utf-8"><title>추천 성능 추가 점검</title><style>body{font:16px/1.6 system-ui;max-width:1100px;margin:40px auto;padding:20px}table{border-collapse:collapse;width:100%}td,th{padding:10px;border-bottom:1px solid #ddd}pre{white-space:pre-wrap}</style><h1>첫 요청·이후 요청과 최근 관심순 비교</h1><p>이미 본 상품과 장바구니 상품을 다시 추천할 수 있는 정책입니다. 최근 관심순은 장바구니 우선, 최근 행동순, 인기순으로 같은 후보를 정렬하는 단순 규칙입니다. 전체 지표를 새로운 상품 발견 성능으로 해석하면 안 됩니다. 구매 정답이 있는 요청에 조건부인 지표이며 전환율이 아닙니다.</p><table><tr><th>요청 구간</th><th>방법</th><th>전체 요청</th><th>구매 정답 요청</th><th>Hit@10</th><th>NDCG@10</th></tr>'+rows+'</table><pre>'+html.escape(json.dumps({k:v['ranker_vs_recent_interest'] for k,v in groups.items()},indent=2))+'</pre></html>'
    (output/'audit.html').write_text(page,'utf-8')
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)


if __name__=='__main__':
    run()
