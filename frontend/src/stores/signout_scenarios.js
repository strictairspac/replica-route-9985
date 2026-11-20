import {signoutScenariosApi} from '../api/signout_scenarios.js';
export const signoutScenariosStore={items:[],async refresh(){this.items=await signoutScenariosApi.list();return this.items;}};
