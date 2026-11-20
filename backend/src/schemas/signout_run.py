from typing import Literal
from pydantic import BaseModel,Field
class SignoutRunInput(BaseModel):
    name:str=Field(min_length=2,max_length=120)
    scenario_id:int=Field(gt=0)
    strategy:Literal['lock_first','recheck_on_commit']
    max_delay_ms:int=Field(default=100,ge=0,le=3600000)
    sticky_ms:int=Field(default=500,ge=0,le=3600000)
