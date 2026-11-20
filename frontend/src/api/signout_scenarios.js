import {api} from './client.js';
export const signoutScenariosApi={list:()=>api('/signout_scenarios'),create:data=>api('/signout_scenarios','POST',data)};
