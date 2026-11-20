"""Deterministic concurrent sign-out drill: shift admission control, idempotent commits and position-based replica reads. Controlled model — never a live lock manager or replica cluster."""
import copy,hashlib,json

MIN_ON_DUTY=1
STRATEGIES=('lock_first','recheck_on_commit')
STRATEGY_LABELS={'lock_first':'查岗时先占住整班','recheck_on_commit':'提交时重查'}
STATUS_LABELS={'waiting':'等待中','pending':'待提交','committed':'成功','rejected':'驳回'}

def simulate(node_inputs,definition,strategy,max_delay_ms,sticky_ms):
    if strategy not in STRATEGIES:raise ValueError('未知签退控制策略：'+str(strategy))
    if not 1<=len(node_inputs)<=12:raise ValueError('需要1–12个节点')
    if len({x['name'] for x in node_inputs})!=len(node_inputs):raise ValueError('节点名必须唯一')
    primaries=[x for x in node_inputs if x['role']=='primary']
    if len(primaries)!=1:raise ValueError('必须且只能配置一个主节点')
    if max_delay_ms<0 or sticky_ms<0:raise ValueError('延迟门槛和粘滞时间不得为负')
    nodes={x['name']:copy.deepcopy(x) for x in node_inputs};primary=nodes[primaries[0]['name']];lineage=primary['lineage']
    if any(n['role']=='replica' and n['lineage']==lineage and n['position']>primary['position'] for n in nodes.values()):raise ValueError('同日志谱系副本位点不得高于主库初始位点')
    shifts=definition.get('shifts') or [];events=definition.get('events') or []
    if not 1<=len(shifts)<=6:raise ValueError('需要1–6个班次')
    if not 1<=len(events)<=200:raise ValueError('需要1–200个时序事件')
    shift_map={};members_seen=set()
    for s in shifts:
        code=s['code'];members=list(s['members'])
        if code in shift_map:raise ValueError('班次编号必须唯一：'+str(code))
        if not 1<=len(members)<=8:raise ValueError('每班需要1–8名成员')
        for m in members:
            if m in members_seen:raise ValueError('成员在多个班次重复：'+str(m))
            members_seen.add(m)
        shift_map[code]={'code':code,'title':s.get('title') or code,'members':members}
    base_position=primary['position'];log=[];requests={};lock_owner={};waiters={c:[] for c in shift_map};deferred={};sessions={};trace=[];last_time=-1

    def roster_at(position):
        gone={e['member'] for e in log if e['position']<=position}
        return {c:[m for m in v['members'] if m not in gone] for c,v in shift_map.items()}
    def emit(row):
        row['index']=len(trace)+1;trace.append(row);return row
    def base_row(at,kind,session):
        return {'at_ms':at,'kind':kind,'session':session,'request_id':None,'shift':None,'member':None,'node':None,'decision':'applied','reason':'','required_position':sessions.get(session,{}).get('required',0),'observed_position':None}

    def drain(shift,at):
        if strategy!='lock_first':return
        while waiters[shift] and not lock_owner.get(shift):
            rid=waiters[shift].pop(0);req=requests[rid];lock_owner[shift]=rid
            req['history'].append('%dms 整班释放，被唤醒并重查'%at)
            row=base_row(at,'signout_begin',req['session']);row.update(request_id=rid,shift=shift,member=req['member'])
            others=[m for m in roster_at(primary['position'])[shift] if m!=req['member']]
            if len(others)>=MIN_ON_DUTY:
                req['status']='pending';req['history'].append('%dms 唤醒后查岗通过，待提交'%at)
                row.update(decision='woken',reason='整班释放，唤醒重查通过：%s 在岗，转为待提交'%('、'.join(others)))
                emit(row)
            else:
                req['status']='rejected';del lock_owner[shift]
                req['history'].append('%dms 唤醒后重查：整班至少留一人，驳回'%at)
                row.update(decision='woken',reason='整班释放，唤醒 %s 并重查'%req['member'])
                emit(row)
                row2=base_row(at,'signout_begin',req['session']);row2.update(request_id=rid,shift=shift,member=req['member'],decision='rejected',reason='整班至少留一人：%s 已是最后一名在岗，驳回签退；位点不推进'%req['member'])
                emit(row2)
        for rid in list(deferred):
            if rid in requests and requests[rid]['shift']==shift and requests[rid]['status']=='pending':
                for ev in deferred.pop(rid):do_commit(ev)

    def do_begin(event):
        at=event['at_ms'];session=event.get('session','default');rid=event.get('request_id');shift=event.get('shift');member=event.get('member')
        if not (rid and shift and member):raise ValueError('signout_begin需要shift、member、request_id')
        if shift not in shift_map:raise ValueError('查岗引用未知班次：'+str(shift))
        if member not in shift_map[shift]['members']:raise ValueError('成员不属于该班次：'+str(member))
        row=base_row(at,'signout_begin',session);row.update(request_id=rid,shift=shift,member=member)
        if rid in requests:
            req=requests[rid]
            row.update(decision='idempotent',reason='重复发起同一请求，返回原受理状态（%s）'%STATUS_LABELS[req['status']],observed_position=req['position'])
            return emit(row)
        req={'request_id':rid,'shift':shift,'member':member,'session':session,'status':'waiting','position':None,'history':[]}
        requests[rid]=req
        on=roster_at(primary['position'])[shift]
        if member not in on:
            req['status']='rejected';req['history'].append('%dms 查岗：已不在岗，驳回'%at)
            row.update(decision='rejected',reason='该成员已不在岗，驳回签退；位点不推进')
            return emit(row)
        if strategy=='lock_first':
            owner=lock_owner.get(shift)
            if owner:
                waiters[shift].append(rid);req['history'].append('%dms 整班被 %s 占住，查岗等待'%(at,owner))
                row.update(decision='waiting',reason='整班已被请求 %s 占住，查岗排队等待'%owner)
                return emit(row)
            lock_owner[shift]=rid;req['history'].append('%dms 占住整班 %s'%(at,shift))
        others=[m for m in on if m!=member]
        if len(others)>=MIN_ON_DUTY:
            req['status']='pending';req['history'].append('%dms 查岗通过：%s 在岗，待提交'%(at,'、'.join(others)))
            row.update(decision='checked',reason=('已占住整班，' if strategy=='lock_first' else '未占班，提交时将重查，')+'查岗通过：%s 在岗'%('、'.join(others)))
            return emit(row)
        req['status']='rejected';req['history'].append('%dms 查岗：整班至少留一人，驳回'%at)
        row.update(decision='rejected',reason='整班至少留一人：%s 是最后一名在岗，驳回签退；位点不推进'%member)
        if strategy=='lock_first' and lock_owner.get(shift)==rid:
            del lock_owner[shift];row['reason']+='；释放整班占用'
        emit(row);drain(shift,at)

    def do_commit(event):
        at=event['at_ms'];rid=event.get('request_id')
        req=requests.get(rid) if rid else None
        if req is None:raise ValueError('提交引用未知请求：'+str(rid))
        session=req['session'];shift=req['shift'];member=req['member']
        row=base_row(at,'signout_commit',session);row.update(request_id=rid,shift=shift,member=member)
        if req['status']=='committed':
            req['history'].append('%dms 提交重试：幂等返回成功'%at)
            row.update(decision='idempotent',reason='提交重试：请求已成功，返回原提交位点 %d，不重复生效'%req['position'],observed_position=req['position'])
        elif req['status']=='rejected':
            req['history'].append('%dms 提交重试：幂等返回驳回'%at)
            row.update(decision='idempotent',reason='提交重试：请求已驳回，维持原结果，位点不推进')
        elif req['status']=='waiting':
            deferred.setdefault(rid,[]).append(event)
            req['history'].append('%dms 提交暂缓：仍在等待占班'%at)
            row.update(decision='deferred',reason='尚未占住整班，提交随唤醒一并处理')
        else:
            if strategy=='lock_first' and lock_owner.get(shift)!=rid:raise ValueError('请求 %s 未占住整班，无法提交'%rid)
            others=[m for m in roster_at(primary['position'])[shift] if m!=member]
            if strategy=='recheck_on_commit' and len(others)<MIN_ON_DUTY:
                req['history'].append('%dms 提交时重查：整班将空，重做查岗'%at)
                emit({**row,'decision':'redo','reason':'提交时重查：%s 签退后整班将空，重做查岗'%member})
                req['status']='rejected';req['history'].append('%dms 重做后仍须留人，驳回'%at)
                row.update(decision='rejected',reason='重做查岗仍不满足整班至少留一人，驳回；位点不推进')
                return emit(row)
            primary['position']+=1;pos=primary['position']
            log.append({'position':pos,'request_id':rid,'shift':shift,'member':member,'session':session,'at_ms':at})
            req['status']='committed';req['position']=pos;req['history'].append('%dms 提交成功，位点 %d'%(at,pos))
            sessions[session]={'required':pos,'last_write_ms':at}
            if strategy=='lock_first':
                del lock_owner[shift]
                row.update(decision='committed',reason='整班已占住，提交直接生效，提交位点推进到 %d；释放整班占用'%pos,observed_position=pos)
            else:
                row.update(decision='committed',reason='提交时重查通过：%s 在岗，签退生效，提交位点推进到 %d'%('、'.join(others),pos),observed_position=pos)
            emit(row);drain(shift,at);return
        emit(row)

    def do_read(event):
        at=event['at_ms'];session=event.get('session','default');shift=event.get('shift')
        if shift is not None and shift not in shift_map:raise ValueError('读取引用未知班次：'+str(shift))
        state=sessions.get(session,{'required':0,'last_write_ms':None});required=state['required']
        row=base_row(at,'read',session);row.update(shift=shift,required_position=required)
        sticky=state['last_write_ms'] is not None and at-state['last_write_ms']<sticky_ms
        eligible=[n for n in nodes.values() if n['role']=='replica' and n['status']=='healthy' and n['lineage']==lineage and n['position']>=required and n['delay_ms']<=max_delay_ms]
        candidates=sorted(eligible,key=lambda n:(n['delay_ms'],-n['position'],n['name']));selected=None
        if sticky:
            if primary['status']=='healthy' and primary['position']>=required:selected=primary;row['reason']='写后读粘滞：保持主库，按最新位点返回在岗人员'
            else:row['reason']='粘滞期主库不可用，拒绝不确定读取'
        elif candidates:selected=candidates[0];row['reason']='副本位点不落后且延迟达标，按已复制位点 %d 返回在岗人员'%selected['position']
        elif primary['status']=='healthy' and primary['position']>=required:selected=primary;row['reason']='副本未追平本会话签退（所需位点 %d），按原规则回退主库'%required
        else:row['reason']='主库不可用且无安全达标副本，拒绝读取'
        row['replica_checks']=[{'name':n['name'],'healthy':n['status']=='healthy','same_lineage':n['lineage']==lineage,'position_sufficient':n['position']>=required,'delay_within_limit':n['delay_ms']<=max_delay_ms} for n in nodes.values() if n['role']=='replica']
        if selected:
            on=roster_at(selected['position']);codes=[shift] if shift else list(shift_map)
            row.update(node=selected['name'],decision='routed',observed_position=selected['position'],roster={c:on[c] for c in codes},safe=selected['position']>=required)
        else:row.update(decision='rejected',roster=None)
        emit(row)

    def do_node(event):
        at=event['at_ms'];kind=event['kind'];target=nodes.get(event.get('node'))
        if not target:raise ValueError('事件引用未知节点：'+str(event.get('node')))
        row=base_row(at,kind,event.get('session','default'));row['node']=target['name']
        if kind=='replicate':
            if target['role']!='replica':raise ValueError('replicate仅允许副本节点')
            if target['status']!='healthy':raise ValueError('停机副本不能应用日志，请先node_up')
            if target['lineage']!=lineage:raise ValueError('日志谱系不匹配，不得复制当前主库日志')
            pos=event.get('position');delay=event.get('delay_ms')
            if pos is None or delay is None or delay<0 or not target['position']<=pos<=primary['position']:raise ValueError('复制位点必须单调且不超过已提交主库位点')
            target['position']=pos;target['delay_ms']=delay
            row.update(decision='applied',reason='副本应用已提交签退日志',observed_position=pos)
        else:
            target['status']='down' if kind=='node_down' else 'healthy'
            row.update(decision='applied',reason='节点下线' if kind=='node_down' else '节点恢复')
        emit(row)

    for event in events:
        at=event['at_ms'];kind=event['kind']
        if at<last_time:raise ValueError('事件必须按时间非递减排列')
        last_time=at
        if kind=='signout_begin':do_begin(event)
        elif kind=='signout_commit':do_commit(event)
        elif kind=='read':do_read(event)
        elif kind in ('replicate','node_down','node_up'):do_node(event)
        else:raise ValueError('未知事件类型：'+str(kind))

    reqs=list(requests.values());final_on=roster_at(primary['position'])
    final_rosters=[{'shift':c,'title':v['title'],'on_duty':final_on[c],'signed_out':[m for m in v['members'] if m not in final_on[c]]} for c,v in shift_map.items()]
    reads=[r for r in trace if r['kind']=='read'];routed=[r for r in reads if r['decision']=='routed']
    summary={'requests':len(reqs),'committed':sum(1 for r in reqs if r['status']=='committed'),'rejected':sum(1 for r in reqs if r['status']=='rejected'),'pending':sum(1 for r in reqs if r['status']=='pending'),'waiting':sum(1 for r in reqs if r['status']=='waiting'),'wait_events':sum(1 for r in trace if r['decision']=='waiting'),'wake_events':sum(1 for r in trace if r['decision']=='woken'),'redo_events':sum(1 for r in trace if r['decision']=='redo'),'idempotent_replays':sum(1 for r in trace if r['decision']=='idempotent'),'commit_position_base':base_position,'commit_position_final':primary['position'],'commits':len(log),'reads':len(reads),'reads_routed':len(routed),'reads_rejected':len(reads)-len(routed),'replica_reads':sum(1 for r in routed if r['node']!=primary['name']),'primary_reads':sum(1 for r in routed if r['node']==primary['name']),'all_reads_safe':all(r.get('safe',True) for r in routed),'min_on_duty_held':all(len(f['on_duty'])>=MIN_ON_DUTY for f in final_rosters)}
    result={'mode':'受控并发签退模型 / 非真实锁与复制集群','strategy':strategy,'strategy_label':STRATEGY_LABELS[strategy],'lineage':lineage,'trace':trace,'requests':reqs,'commit_log':log,'final_rosters':final_rosters,'sessions':sessions,'final_nodes':list(nodes.values()),'summary':summary,'policy':{'min_on_duty':MIN_ON_DUTY,'max_delay_ms':max_delay_ms,'sticky_ms':sticky_ms,'position_model':'同一命名日志谱系的单调整数位点；驳回与幂等重试不推进位点','idempotency':'按 request_id 幂等：重试返回原结果，不重复生效','sticky_boundary':'at_ms - last_write_ms < sticky_ms','replica_tie_break':'delay升序 / position降序 / name字典序'}}
    result['checksum']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest();return result
