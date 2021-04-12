import Fetch from "./Fetch.service.js";
import Actions from "./Action.service.js";

const DEFAULT_INSTANCE = {
  currentMainPageModal: null,
};

const Store = (function () {
  var instance = {
    checkInterleave : {},
  };
  let modalId = "connectToDbModal";

  function init() {}

  return {
    getinstance: function () {
      return instance;
    },

    // Other store manipulator functions here
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
      instance = { ...instance, open: openVal };
    },
    updateSchemaScreen: async (tableData) => {
      localStorage.setItem("conversionReportContent", tableData);
      await Actions.ddlSummaryAndConversionApiCall();
      instance = { ...instance, tableData, saveSchemaId: Math.random() };
    },
    setInterleave : (tableName , value) => {
      let newCheckInterLeave = instance.checkInterleave;
      newCheckInterLeave[tableName] = value
      instance = {...instance, checkInterleave : newCheckInterLeave };
      // console.log(instance);
    }
  };
})();

export default Store;
