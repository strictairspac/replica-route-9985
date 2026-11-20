from fastapi import APIRouter,Depends
from fastapi.responses import Response
import json
from src.config.database import db_session
from src.utils.security import current,writer
from src.schemas.signout_run import SignoutRunInput
from src.services import signout_runs as service
router=APIRouter()
@router.get('/signout_runs')
def listing(db=Depends(db_session),user=Depends(current)):return service.listing(db)
@router.post('/signout_runs',status_code=201)
def create(data:SignoutRunInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,data,user)
@router.get('/signout_runs/{ident}')
def detail(ident:int,db=Depends(db_session),user=Depends(current)):return service.detail(db,ident)
@router.get('/signout_runs/{ident}/export')
def export(ident:int,db=Depends(db_session),user=Depends(current)):
    return Response(json.dumps(service.detail(db,ident),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="signout-{ident}.json"'})
