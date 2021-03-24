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
        closeModal: (id) => {
            Store.setCurrentModal(id);
        },
        openModal1: () => {
            Store.setCurrentModal('modal1');
        }
    }
})();

export default Actions;