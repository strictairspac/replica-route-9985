import {signoutRunsApi} from '../api/signout_runs.js';
export const signoutRunsStore={items:[],async refresh(){this.items=await signoutRunsApi.list();return this.items;}};
