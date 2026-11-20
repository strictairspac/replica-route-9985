import traceback,json,os
from pathlib import Path
from src.config.database import Session
from src.models.entities import Trial,SignoutRun
from src.services.trials import queue
from src.services.routing import simulate as route_simulate
from src.services.signout import simulate as signout_simulate
from src.services.signout_runs import QUEUE_KEY as SIGNOUT_QUEUE
ARTIFACTS=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))

def run_trial(ident):
    with Session() as db:
        obj=db.get(Trial,ident);obj.status='running';db.commit();snapshot=obj.input_snapshot;delay=obj.max_delay_ms;sticky=obj.sticky_ms
    result=route_simulate(snapshot['nodes'],snapshot['request_set']['events'],delay,sticky)
    with Session() as db:
        obj=db.get(Trial,ident);obj.result=result;obj.status='completed';db.commit()
    (ARTIFACTS/f'route-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))

def run_signout(ident):
    with Session() as db:
        obj=db.get(SignoutRun,ident);obj.status='running';db.commit();snapshot=obj.input_snapshot;strategy=obj.strategy;delay=obj.max_delay_ms;sticky=obj.sticky_ms
    result=signout_simulate(snapshot['nodes'],snapshot['scenario']['definition'],strategy,delay,sticky)
    with Session() as db:
        obj=db.get(SignoutRun,ident);obj.result=result;obj.status='completed';db.commit()
    (ARTIFACTS/f'signout-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))

def main():
    ARTIFACTS.mkdir(parents=True,exist_ok=True)
    while True:
        Path('/tmp/worker-heartbeat').touch()
        item=queue.blpop(['replicaroute:queue',SIGNOUT_QUEUE],timeout=2)
        if not item:continue
        key,raw=item;ident=int(raw);signout=key==SIGNOUT_QUEUE
        try:
            run_signout(ident) if signout else run_trial(ident)
        except Exception as e:
            traceback.print_exc()
            with Session() as db:
                obj=db.get(SignoutRun if signout else Trial,ident)
                if obj:obj.status='failed';obj.error=str(e);db.commit()

if __name__=='__main__':main()
