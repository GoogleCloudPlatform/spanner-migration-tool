import Fetch from "./Fetch.service.js";
import Actions from "./Action.service.js";

const DEFAULT_INSTANCE = {
  currentMainPageModal: null,
};

const Store = (function () {
  var instance = {
    checkInterleave : {},
    currentTab:"reportTab",
    openStatus:{
      ddl:new Array(16).fill(false),
      report: new Array(16).fill(false),
      summary:new Array(16).fill(false),
    },
    tableData:{
      reportTabContent: JSON.parse(localStorage.getItem('conversionReportContent')),
      ddlTabContent:  JSON.parse(localStorage.getItem('ddlStatementsContent')),
      summaryTabContent:  JSON.parse(localStorage.getItem('summaryReportContent'))
    },
    tableBorderData: JSON.parse(localStorage.getItem("tableBorderColor")),
   };
   console.log(instance);
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
      instance = { ...instance, tableData, saveSchemaId: Math.random()};
    },
    setInterleave : (tableName , value) => {
      let newCheckInterLeave = instance.checkInterleave;
      newCheckInterLeave[tableName] = value
      instance = {...instance, checkInterleave:newCheckInterLeave}; 
    },
    swithCurrentTab:(tab)=>{
      console.log(tab);
      instance = {...instance , currentTab:tab}
    },
    openCarousel:(tableId , tableIndex)=>{
      console.log('open',tableId , tableIndex);
      instance.openStatus[tableId][tableIndex] = true;
    },
    closeCarousel:(tableId , tableIndex)=>{
      console.log('close',tableId , tableIndex);
      instance.openStatus[tableId][tableIndex] = false;
    },
    getTableData: (tabName)=>{
      return JSON.parse(instance.tableData[tabName + "Content"]);
    }

 };
})();

export default Store;
