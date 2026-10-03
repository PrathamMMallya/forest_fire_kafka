import os, json
from pathlib import Path
BASE = Path('mlruns')

def parse_meta(mfile):
    d={}
    try:
        for ln in mfile.read_text().splitlines():
            if ':' in ln:
                k,v = ln.split(':',1)
                d[k.strip()] = v.strip().strip("'")
    except Exception:
        pass
    return d

out=[]
if not BASE.exists():
    print('no mlruns dir')
else:
    for exp in BASE.iterdir():
        if not exp.is_dir():
            continue
        for run in exp.iterdir():
            if not run.is_dir():
                continue
            meta=parse_meta(run/ 'meta.yaml') if (run/'meta.yaml').exists() else {}
            metrics={}
            mdir=run/'metrics'
            if mdir.exists():
                for m in mdir.iterdir():
                    try:
                        lines = [ln.strip() for ln in m.read_text().splitlines() if ln.strip()]
                        metrics[m.name]=len(lines)
                    except Exception as e:
                        metrics[m.name]='err'
            out.append({'exp':exp.name,'run':run.name,'meta':meta,'metrics':metrics})
print(json.dumps(out[:20],indent=2))
