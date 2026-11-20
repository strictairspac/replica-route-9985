from fastapi import APIRouter,Depends
from src.config.database import db_session
from src.utils.security import current,writer
from src.schemas.signout_scenario import SignoutScenarioInput
from src.services import signout_scenarios as service
router=APIRouter()
@router.get('/signout_scenarios')
def listing(db=Depends(db_session),user=Depends(current)):return service.listing(db)
@router.post('/signout_scenarios',status_code=201)
def create(data:SignoutScenarioInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,data,user)
@router.put('/signout_scenarios/{ident}')
def update(ident:int,data:SignoutScenarioInput,db=Depends(db_session),user=Depends(writer)):return service.update(db,ident,data,user)
