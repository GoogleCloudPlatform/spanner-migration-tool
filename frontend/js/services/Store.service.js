// import the fetch service here
import Fetch from "./Fetch.service.js";

const DEFAULT_INSTANCE = {
  currentMainPageModal: null, // "name", null
};

const Store = (function () {
  var instance;
  

  function init() {
    // the initial data from the fetch service
    Fetch.getData().then((data) => {
      instance = data;
     
    });
  }

  return {
    getinstance: function () {
      if (!instance) {
        instance = init();
      }
     
      return instance;
    },
    // Other store maniuolator functions here
    // may be later can be moved to actions and stiched to affect the store
    addAttrToStore: () => {
      if (!instance) {
        return;
      }
      instance = { ...instance, something: "of value" };
    },
    toggleStore: () => {
      if (!instance) {
        return;
      }
      let openVal = instance.open;
      if (instance.open === "no") {
        openVal = "yes";
      } else {
        openVal = "no";
      }
      instance = { ...instance, open: openVal };
    },
    setCurrentModal: (currentModal) => {
    //   instance = { ...instance, currentModal };
    },
    changeCurrentTab: (currentTab) => {
      instance = { ...instance, currentTab };
    },
    getOpenTab: () => {
      return instance.currentTab;
    },
  };
})();

export default Store;
