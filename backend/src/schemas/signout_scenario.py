from typing import Literal
from pydantic import BaseModel,Field,model_validator
class ShiftDef(BaseModel):
    code:str=Field(min_length=1,max_length=40)
    title:str=Field(min_length=1,max_length=80)
    members:list[str]=Field(min_length=1,max_length=8)
class SignoutEvent(BaseModel):
    at_ms:int=Field(ge=0,le=86400000)
    kind:Literal['signout_begin','signout_commit','read','replicate','node_down','node_up']
    session:str=Field(default='default',min_length=1,max_length=60)
    shift:str|None=Field(default=None,max_length=40)
    member:str|None=Field(default=None,max_length=40)
    request_id:str|None=Field(default=None,min_length=1,max_length=60)
    node:str|None=Field(default=None,max_length=80)
    position:int|None=Field(default=None,ge=0,le=1000000000)
    delay_ms:int|None=Field(default=None,ge=0,le=3600000)
    @model_validator(mode='after')
    def valid_event(self):
        if self.kind=='signout_begin' and not (self.shift and self.member and self.request_id):raise ValueError('signout_begin需要shift、member、request_id')
        if self.kind=='signout_commit' and not self.request_id:raise ValueError('signout_commit需要request_id')
        if self.kind in ('replicate','node_down','node_up') and not self.node:raise ValueError('节点事件需要node')
        if self.kind=='replicate' and (self.position is None or self.delay_ms is None):raise ValueError('replicate需要position和delay_ms')
        return self
class SignoutDefinition(BaseModel):
    shifts:list[ShiftDef]=Field(min_length=1,max_length=6)
    events:list[SignoutEvent]=Field(min_length=1,max_length=200)
    @model_validator(mode='after')
    def chronological(self):
        if any(a.at_ms>b.at_ms for a,b in zip(self.events,self.events[1:])):raise ValueError('事件必须按at_ms非递减排列')
        return self
class SignoutScenarioInput(BaseModel):
    code:str=Field(min_length=2,max_length=80)
    title:str=Field(min_length=2,max_length=160)
    definition:SignoutDefinition
    note:str=Field(default='',max_length=300)
