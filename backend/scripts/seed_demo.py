from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.models.entities import User,Node,RequestSet,Trial,SignoutScenario,SignoutRun
from src.utils.security import password_hash

SIGNOUT_DEF={'shifts':[{'code':'alpha','title':'航道一班','members':['林澜','沈屿']},{'code':'beta','title':'航道二班','members':['周衡','陆苇','柯桥']}],'events':[
    {'at_ms':0,'kind':'signout_begin','session':'duty-A','shift':'alpha','member':'林澜','request_id':'req-alpha-lin'},
    {'at_ms':10,'kind':'signout_begin','session':'duty-B','shift':'alpha','member':'沈屿','request_id':'req-alpha-shen'},
    {'at_ms':20,'kind':'signout_begin','session':'duty-C','shift':'beta','member':'周衡','request_id':'req-beta-zhou'},
    {'at_ms':30,'kind':'signout_commit','request_id':'req-beta-zhou'},
    {'at_ms':40,'kind':'signout_commit','request_id':'req-alpha-lin'},
    {'at_ms':50,'kind':'signout_commit','request_id':'req-alpha-shen'},
    {'at_ms':55,'kind':'signout_commit','request_id':'req-alpha-shen'},
    {'at_ms':60,'kind':'signout_commit','request_id':'req-alpha-lin'},
    {'at_ms':70,'kind':'read','session':'duty-A','shift':'alpha'},
    {'at_ms':80,'kind':'read','session':'visitor','shift':'alpha'},
    {'at_ms':550,'kind':'read','session':'duty-A','shift':'alpha'},
    {'at_ms':600,'kind':'replicate','node':'south-replica','position':107,'delay_ms':20},
    {'at_ms':610,'kind':'read','session':'duty-B','shift':'alpha'},
    {'at_ms':620,'kind':'read','session':'duty-A','shift':'alpha'}]}

def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed),('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}]),('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}])]
        for code,title,events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note='栖原站点值班交接 · 09:15 请求轨迹'))
        if not db.scalar(select(SignoutScenario).where(SignoutScenario.code=='QY-SO-A')):db.add(SignoutScenario(code='QY-SO-A',title='同班两人并发签退交错',definition=SIGNOUT_DEF,note='两人查岗交错在任何提交之前；含幂等重试、别班照常与副本追平读取，两种占班策略可对照'))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial,SignoutScenario,SignoutRun)})
if __name__=='__main__':seed()
