from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import Node,SignoutScenario,SignoutRun
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from .common import save
from .signout import simulate
from .trials import queue
QUEUE_KEY='replicaroute:signout-queue'
def listing(db):return [record(x) for x in db.scalars(select(SignoutRun).order_by(SignoutRun.id.desc()))]
def detail(db,ident):
    obj=db.get(SignoutRun,ident)
    if not obj:raise HTTPException(404,'签退演练不存在')
    return record(obj)
def create(db,data,user):
    scenario=db.get(SignoutScenario,data.scenario_id)
    if not scenario:raise HTTPException(404,'签退场景不存在')
    nodes=[record(x) for x in db.scalars(select(Node).order_by(Node.id))]
    try:simulate(nodes,scenario.definition,data.strategy,data.max_delay_ms,data.sticky_ms)
    except ValueError as e:raise HTTPException(422,str(e))
    snapshot={'nodes':nodes,'scenario':record(scenario)};obj=SignoutRun(**data.model_dump(),input_snapshot=snapshot,status=RUN_STATES[0]);result=save(db,obj,user,'签退演练入队 '+data.name)
    try:queue.rpush(QUEUE_KEY,str(obj.id))
    except Exception:
        obj.status='failed';obj.error='队列暂时不可用，请重新创建';db.commit();raise HTTPException(503,obj.error)
    return result
