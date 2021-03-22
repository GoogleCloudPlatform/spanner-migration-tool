import Store from "./Store.service.js";

/**
 * All the manioulations to the store happen via the actions mentioned in this module
 * 
 */
const Actions = (() => {

    return {
        trial: () => {
            console.log(' this was the trial in the actions ');
            return '1';
        },
        addAttrToStore: () => {
            Store.addAttrToStore();
        },
        closeStore: () => {
            Store.toggleStore();
        },
        addNewSession: (session) =>{
            Store.addNewSession(session);
        },
        resumeSession: (index) => {
           let val=Store.getSessionData(index);
           console.log(val);
        //    return val;
        },
        getAllSessions: () =>{
            return Store.getAllSessions();

        }
    }
})();

export default Actions;