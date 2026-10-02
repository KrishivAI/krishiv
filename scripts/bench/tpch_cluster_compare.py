"""cluster_compare.py CLUSTER.json LOCAL.json — digest + topology diff of a distributed run against an embedded one."""
import json,sys
c=json.load(open(sys.argv[1])); l=json.load(open(sys.argv[2]))
assert c.get("digest_scheme",2)==l.get("digest_scheme"), "digest scheme mismatch"
local={q["id"]:q for q in l["engines"]["krishiv"]}
rows=c.get("queries") or c.get("results") or []
same=order=diff=fail=0; single=[]
print("| q | status | cluster s | local s | stages | tasks | answer |\n|---|---|---|---|---|---|---|")
for q in rows:
    i=int(str(q["id"]).lstrip("q")); lq=local.get(i,{})
    if q["status"]!="ok": fail+=1; verdict=q.get("error","")[:60]
    elif q.get("digest")==lq.get("digest"): same+=1; verdict="same"
    elif q.get("digest_unordered")==lq.get("digest_unordered"): order+=1; verdict="same rows, tie order"
    else: diff+=1; verdict=f"DIFF rows={q.get('rows')} vs {lq.get('rows')}"
    if q.get("task_count",2)<=1: single.append(q["id"])
    print(f"| q{i} | {q['status']} | {q.get('elapsed_s',0):.1f} | {lq.get('elapsed_s',0):.1f} | {q.get('stage_count')} | {q.get('task_count')} | {verdict} |")
print(f"\nidentical {same}, tie-order {order}, DIFFERENT {diff}, failed {fail}; ran as a single task: {single or 'none'}")
