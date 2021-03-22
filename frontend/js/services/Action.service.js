import Store from "./Store.service.js";

/**
 * All the manipulations to the store happen via the actions mentioned in this module
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
        openConnectionModal: () => {
            console.log("connect modal opened");
        }
    }
})();

export default Actions;