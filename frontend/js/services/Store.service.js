import Fetch from "./Fetch.service.js";
import Actions from "./Action.service.js";

const DEFAULT_INSTANCE = {
  currentMainPageModal: null,
};

const Store = (function () {
  var instance = {
    checkInterleave : {},
    currentTab:"reportTab",
    sourceDbName:'',
    globalDbType:'',
    openStatus:{
      ddl:new Array(16).fill(false),
      report: new Array(16).fill(false),
      summary:new Array(16).fill(false),
    },
    searchInputValue :{
      ddlTab:'',
      reportTab:'',
      summaryTab:''
    },
    tableData:{
      reportTabContent: {},
      ddlTabContent: {},
      summaryTabContent: {}
    },
    tableBorderData:{},
    globalDataTypeList:{},
   };
  let modalId = "connectToDbModal";
  let checkInterLeaveArray = {};

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
      Store.updateTableData("reportTabContent",tableData);   
      await Actions.ddlSummaryAndConversionApiCall();
      instance = { ...instance, tableData, saveSchemaId: Math.random()};
    },
    setInterleave : (tableName , value) => {
      checkInterLeaveArray[tableName] = value;
      if(Object.keys(checkInterLeaveArray).length==16){
      instance = {...instance, checkInterleave:checkInterLeaveArray}; 
      }
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
    },
    updateTableData:(key , data)=>{
      instance.tableData[key] = data;
    },
    updateTableBorderData:(data)=>{
      instance.tableBorderData = data;
    },
    expandAll: (value) => {
      let key = instance.currentTab.substr(0,instance.currentTab.length-3);
      console.log(key);
      instance.openStatus[key].fill(value);
    },
    setSourceDbName:(name)=>{
      instance.sourceDbName = name;
    },
    getSourceDbName:()=>{
      return instance.sourceDbName
    },
    setGlobalDbType:(value)=>{
      instance.globalDbType = value;
    },
    getGlobalDbType:()=>{
      return instance.globalDbType;
    },
    setGlobalDataTypeList:(value)=>{
      instance.globalDataTypeList = value
    },
    getGlobalDataTypeList:()=>{
        return instance.globalDataTypeList;
    },
    setSearchInputValue :(key,value)=>{
      instance.searchInputValue[key]=value;
    },
    getSearchInputValue:(key)=>{
      return instance.searchInputValue[key];
    }

 };
})();

export default Store;
