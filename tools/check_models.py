import json
from inference import app

def main():
    try:
        d = app.list_models()
        print('models_count:', len(d))
        print(json.dumps(d[:5], indent=2))
        if d:
            rid = d[0]['run_id']
            print('\nFetching metrics for run', rid)
            m = app.get_run_metrics(rid)
            for k,v in m.items():
                print(k, 'points:', len(v))
    except Exception as e:
        print('ERR', e)

if __name__ == '__main__':
    main()
