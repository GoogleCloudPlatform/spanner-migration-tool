const DEFAULT_INSTANCE = {
  currentMainPageModal: null,
};

const Store = (function () {
  var tableChanges = "collapse";
  var pageYOffset = 0;
  var instance = {
    checkInterleave: {},
    currentTab: "reportTab",
    sourceDbName: "",
    openStatus: {
      ddl: new Array(1).fill(false),
      report: new Array(1).fill(false),
      summary: new Array(1).fill(false),
    },
    tableMode: new Array(1).fill(false),
    currentPageNumber : new Array(1).fill(0),
    searchInputValue: {
      ddlTab: "",
      reportTab: "",
      summaryTab: "",
    },
    tableData: {
      reportTabContent: {},
      ddlTabContent: {},
      summaryTabContent: {},
    },
    tableBorderData: {},
    globalDataTypeList: {},
  };
  let checkInterLeaveArray = {};

  return {
    getinstance: function () {
      return instance;
    },

    setPageYOffset:(value)=>{
      pageYOffset = value;
    },

    getPageYOffset:()=>{
      return pageYOffset;
    },

    getTableChanges:()=>{
      return tableChanges;
    },

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

    setarraySize: (val) => {
      instance.openStatus = {
        ddl: new Array(val).fill(false),
        report: new Array(val).fill(false),
        summary: new Array(val).fill(false),
      };
      instance.currentPageNumber=new Array(val).fill(0),
      instance.tableMode = new Array(val).fill(false);
    },

    setInterleave: (tableName, value) => {
     instance.checkInterleave[tableName] = value;
    },

    switchCurrentTab: (tab) => {
      instance = { ...instance, currentTab: tab };
    },

    openCarousel: (tableId, tableIndex) => {
      instance.openStatus[tableId][tableIndex] = true;
    },

    closeCarousel: (tableId, tableIndex) => {
      instance.openStatus[tableId][tableIndex] = false;
    },

    incrementPageNumber:(tableIndex)=>{
      instance.currentPageNumber[tableIndex]++;
    },

    decrementPageNumber:(tableIndex)=>{
      instance.currentPageNumber[tableIndex]--;
    },

    getCurrentPageNumber:(tableIndex)=>{
      return instance.currentPageNumber[tableIndex]
    },

    getTableData: (tabName) => {
      return JSON.parse(instance.tableData[tabName + "Content"]);
    },

    updatePrimaryKeys: (tableData) => {
      let numOfSpannerTables = Object.keys(tableData.SpSchema).length;
      for (let x = 0; x < numOfSpannerTables; x++) {
        let spannerTable =
          tableData.SpSchema[Object.keys(tableData.SpSchema)[x]];
        let pkSeqId = 1;
        for (let y = 0; y < spannerTable.Pks.length; y++) {
          if (spannerTable.Pks[y].seqId == undefined) {
            spannerTable.Pks[y].seqId = pkSeqId;
            pkSeqId++;
          }
        }
      }
    },

    updateTableData: (key, data) => {
      instance.tableData[key] = data;
    },

    updateTableBorderData: (data) => {
      instance.tableBorderData = data;
    },

    collapseAll:(value) => {
      let key = instance.currentTab.substr(0, instance.currentTab.length - 3);
      tableChanges = "collapse"
      instance.openStatus[key].fill(value);
    },

    expandAll: (x,y) => {
      let key = instance.currentTab.substr(0, instance.currentTab.length - 3);     
      tableChanges = "expand";
      let openStatusarray = instance.openStatus[key];
      for(let i =0; i<openStatusarray.length;i++)
      {
        let bottomval = document.getElementById(i).getBoundingClientRect().bottom;
        let topval = document.getElementById(i).getBoundingClientRect().y;
    
        if(bottomval<-1000 || topval> window.innerHeight +1000)
        {
          openStatusarray[i] = false;
        }
        else
        {
          openStatusarray[i] = true;
        }
      }
    },

    setSourceDbName: (name) => {
      instance.sourceDbName = name;
    },

    getSourceDbName: () => {
      return instance.sourceDbName;
    },

    setGlobalDbType: (value) => {
      instance.globalDbType = value;
    },

    getGlobalDbType: () => {
      return instance.globalDbType;
    },

    setGlobalDataTypeList: (value) => {
      instance.globalDataTypeList = value;
    },

    getGlobalDataTypeList: () => {
      return instance.globalDataTypeList;
    },

    setSearchInputValue: (key, value) => {
      instance.searchInputValue[key] = value;
    },

    getSearchInputValue: (key) => {
      return instance.searchInputValue[key];
    },

    resetStore: () => {
      tableChanges = "collapse";
      checkInterLeaveArray = {};
      instance = {
        checkInterleave: {},
        currentTab: "reportTab",
        sourceDbName: "",
        openStatus: {
          ddl: new Array(1).fill(false),
          report: new Array(1).fill(false),
          summary: new Array(1).fill(false),
        },
        tableMode: new Array(1).fill(false),
        currentPageNumber : new Array(1).fill(0),
        searchInputValue: {
          ddlTab: "",
          reportTab: "",
          summaryTab: "",
        },
        tableData: {
          reportTabContent: {},
          ddlTabContent: {},
          summaryTabContent: {},
        },
        tableBorderData: {},
        globalDataTypeList: {},
      };
    },

    getCurrentTab: () => {
      return instance.currentTab;
    },

    getInterleaveConversionForATable: (tableName) => {
      return instance.checkInterleave[tableName];
    },

    getTableMode: (tableIndex) => {
      return instance.tableMode[tableIndex];
    },

    setTableMode: (tableIndex, val) => {
      instance.tableMode[tableIndex] = val;
    },

    setPageNumber: (tableIndex, pageNumber) => {
      instance.currentPageNumber[tableIndex] = pageNumber;
    }
  };
})();

export default Store;
