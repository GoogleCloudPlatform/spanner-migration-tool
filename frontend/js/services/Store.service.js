// import the fetch service here
import Fetch from "./Fetch.service.js";

const DEFAULT_INSTANCE = {
    currentMainPageModal: null, // "name", null
    
}

const Store = (function() {

    var instance;
    let sessionData = [
        {
          sessionName : "File1.json",
          sessionDate : "2017-01-01" ,
          sessionTime : "1:2:3",
          sessionAction : "resume_sesssion_url"
      },
      {
        sessionName : "File1.json",
        sessionDate : "2017-01-01" ,
        sessionTime : "1:2:3",
        sessionAction : "resume_sesssion_url"
      },
      ]
      let modalId = "connectToDbModal"

    function init() {
        // the initial data from the fetch service
        Fetch.getData().then((data) => {
            instance = data;
        });
    }

    return {
        setModalId : function(id) {
            modalId = id;
        },
        getModalId : function() {
            return modalId;
        },
        getinstance: function() {
            if (!instance) {
                instance = init();
            }
            return instance;
        },
        // Other store maniuolator functions here 
        // may be later can be moved to actions and stiched to affect the store
        addAttrToStore: () => {
            if (!instance) { return; }
            instance = {...instance, something: 'of value' }
        },
        toggleStore: () => {
            if (!instance) { return; }
            let openVal = instance.open;
            if (instance.open === 'no') {
                openVal = 'yes';
            } else {
                openVal = 'no';
            }
            instance = {...instance, open: openVal };
        },
        addNewSession: (session) => {
            sessionData.append(session);
        },
        getSessionData: (index) => {
            return sessionData[index];
        },
        getAllSessions : () => {
            return sessionData;
        }
    };
})();

export default Store;