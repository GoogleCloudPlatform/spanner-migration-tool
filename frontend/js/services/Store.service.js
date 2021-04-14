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
      ddl:new Array(1).fill(false),
      report : new Array(1).fill(false),
      summary: new Array(1).fill(false)
    },
    tableData:{
      reportTabContent: JSON.parse(localStorage.getItem('conversionReportContent')),
      ddlTabContent:  JSON.parse(localStorage.getItem('ddlStatementsContent')),
      summaryTabContent:  JSON.parse(localStorage.getItem('summaryReportContent'))
    },
    tableBorderData: JSON.parse(localStorage.getItem("tableBorderColor")),
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
      instance = { ...instance, tableData, saveSchemaId: Math.random()};
    },
    setInterleave : (tableName , value) => {
      let newCheckInterLeave = instance.checkInterleave;
      newCheckInterLeave[tableName] = value
      instance = {...instance, checkInterleave:newCheckInterLeave , saveSchemaId: Math.random()  }; 
    },
    swithCurrentTab:(tab)=>{
      debugger
      console.log(tab);
      instance = {...instance , currentTab:tab}
    },
    openCarousel:(tableId , tableIndex)=>{
      debugger
      console.log('open',tableId , tableIndex);
      let newOpenStatus = instance.openStatus
      newOpenStatus[tableId][tableIndex] = true;
      instance = {...instance ,openStatus:newOpenStatus }
      console.log(instance);
    },
    closeCarousel:(tableId , tableIndex)=>{
      console.log('close',tableId , tableIndex);
      let newOpenStatus = instance.openStatus
      newOpenStatus[tableId][tableIndex] = false;
      instance = {...instance ,openStatus:newOpenStatus }
      console.log(instance);
    },
    getTableData: (tabName)=>{
      return JSON.parse(instance.tableData[tabName + "Content"]);
    }

 };
})();

export default Store;
