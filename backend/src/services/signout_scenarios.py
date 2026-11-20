from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import SignoutScenario
from src.utils.serialization import record
from .common import save
def listing(db):return [record(x) for x in db.scalars(select(SignoutScenario).order_by(SignoutScenario.id))]
def create(db,data,user):return save(db,SignoutScenario(**data.model_dump()),user,'创建签退场景 '+data.code)
def update(db,ident,data,user):
    obj=db.get(SignoutScenario,ident)
    if not obj:raise HTTPException(404,'签退场景不存在')
    for k,v in data.model_dump().items():setattr(obj,k,v)
    return save(db,obj,user,'更新签退场景 '+data.code)
