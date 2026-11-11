"""查看请求到达分布及已有服务时长表。"""
from collections import Counter
from .io import unique
def summarize(value):
    rows=value['requests'];unique(rows,'id');types=Counter()
    for row in rows:
        if type(row['arrival']) is not int or type(row['deadline']) is not int or row['arrival']<0 or row['deadline']<row['arrival'] or row['type'] not in value['service']:raise ValueError('请求字段错误')
        types[row['type']]+=1
    for table in value['service'].values():
        if not table or any(type(x) is not int or x<=0 for x in table.values()):raise ValueError('服务时间错误')
    return {'request_count':len(rows),'types':dict(sorted(types.items())),'first_arrival':min((x['arrival'] for x in rows),default=None),'last_arrival':max((x['arrival'] for x in rows),default=None)}
