import {api} from './client.js';
export const signoutRunsApi={list:()=>api('/signout_runs'),create:data=>api('/signout_runs','POST',data)};
