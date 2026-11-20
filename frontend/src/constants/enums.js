export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const RUN_STATES={queued:'排队中',running:'执行中',completed:'已完成',failed:'失败'};
export const STRATEGIES={lock_first:'查岗时先占住整班',recheck_on_commit:'提交时重查'};
export const SIGNOUT_STATES={waiting:'等待中',pending:'待提交',committed:'成功',rejected:'驳回'};
export const DECISIONS={checked:'查岗通过',waiting:'等待占班',woken:'唤醒重查',committed:'提交成功',rejected:'驳回',idempotent:'幂等重放',redo:'重查重做',deferred:'暂缓提交',routed:'路由成功',applied:'已应用'};
export const EVENT_KINDS={signout_begin:'查岗',signout_commit:'提交',read:'副本读取',replicate:'日志复制',node_down:'节点下线',node_up:'节点恢复'};
