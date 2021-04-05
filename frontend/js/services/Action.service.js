import Store from "./Store.service.js";
import Fetch from "./Fetch.service.js";
import {
  readTextFile,
  showSnackbar,
  tabbingHelper,
} from "./../helpers/SchemaConversionHelper.js";
var keysList = [];
var orderId = 0;
var TEMP = {}
/**
 * All the manipulations to the store happen via the actions mentioned in this module
 *
 */
const Actions = (() => {
  return {
    trial: () => {
      console.log(" this was the trial in the actions ");
      return "1";
    },
    addAttrToStore: () => {
      Store.addAttrToStore();
    },
    closeStore: () => {
      Store.toggleStore();
    },
    onLoadDatabase: async (dbType, dumpFilePath) => {
      let reportData,
        sourceTableFlag,
        reportDataResp,
        reportDataCopy,
        jsonReportDataResp,
        requestCode;
      reportData = await Fetch.getAppData("POST", "/convert/dump", {
        Driver: dbType,
        Path: dumpFilePath,
      });
      reportDataCopy = reportData.clone();
      requestCode = reportData.status;
      reportDataResp = await reportData.text();
      if (requestCode != 200) {
        Fetch.showSnackbar(reportDataResp, " redBg");
        return false;
      } else {
        jsonReportDataResp = await reportDataCopy.json();
        console.log(jsonReportDataResp);
        if (Object.keys(jsonReportDataResp.SpSchema).length == 0) {
          showSnackbar("Please select valid file", " redBg");
          return false;
        } else {
          // showSpinner();
          jQuery("#loadDatabaseDumpModal").modal("hide");
          localStorage.setItem("conversionReportContent", reportDataResp);
        }
      }
      sourceTableFlag = localStorage.getItem("sourceDbName");
      // sessionRetrieval(sourceTableFlag);
      return true;
    },
    onconnect: async (dbType, dbHost, dbPort, dbUser, dbName, dbPassword) => {
      let sourceTableFlag = "",
        response;
      let payload = {
        Driver: dbType,
        Database: dbName,
        Password: dbPassword,
        User: dbUser,
        Port: dbPort,
        Host: dbHost,
      };
      response = await Fetch.getAppData("POST", "/connect", payload);
      if (response.ok) {
        if (dbType === "mysql") sourceTableFlag = "MySQL";
        else if (dbType === "postgres") sourceTableFlag = "Postgres";
        localStorage.setItem("sourceDbName", sourceTableFlag);
        jQuery("#connectToDbModal").modal("hide");
        jQuery("#connectModalSuccess").modal();
      } else {
        jQuery("#connectToDbModal").modal("hide");
        jQuery("#connectModalFailure").modal();
      }
      return response;
    },
    showSchemaAssessment: async () => {
      let reportDataResp, reportData, sourceTableFlag;
      reportData = await Fetch.getAppData("GET", "/convert/infoschema");
      reportDataResp = await reportData.text();
      localStorage.setItem("conversionReportContent", reportDataResp);
      jQuery("#connectModalSuccess").modal("hide");
      sourceTableFlag = localStorage.getItem("sourceDbName");
    },
    ddlSummaryAndConversionApiCall: async () => {
      let conversionRate,
        conversionRateJson,
        ddlData,
        ddlDataJson,
        summaryData,
        summaryDataJson;
      ddlData = await Fetch.getAppData("GET", "/ddl");
      summaryData = await Fetch.getAppData("GET", "/summary");
      conversionRate = await Fetch.getAppData("GET", "/conversion");
      if (ddlData.ok && summaryData.ok && conversionRate.ok) {
        ddlDataJson = await ddlData.json();
        summaryDataJson = await summaryData.json();
        conversionRateJson = await conversionRate.json();
        localStorage.setItem(
          "ddlStatementsContent",
          JSON.stringify(ddlDataJson)
        );
        localStorage.setItem(
          "summaryReportContent",
          JSON.stringify(summaryDataJson)
        );
        localStorage.setItem(
          "tableBorderColor",
          JSON.stringify(conversionRateJson)
        );
      } else {
        return false;
      }
      return true;
    },
    sessionRetrieval: async (dbType) => {
      let sessionStorageArr, sessionInfo, sessionResp;
      sessionResp = await Fetch.getAppData("GET", "/session");
      sessionInfo = await sessionResp.json();
      sessionStorageArr = JSON.parse(sessionStorage.getItem("sessionStorage"));
      if (sessionStorageArr == undefined) sessionStorageArr = [];
      sessionInfo.sourceDbType = dbType;
      sessionStorageArr.unshift(sessionInfo);
      sessionStorage.setItem(
        "sessionStorage",
        JSON.stringify(sessionStorageArr)
      );
    },
    resumeSessionHandler: async (index, sessionArray) => {
      let driver, path, dbName, sourceDb, pathArray, fileName, filePath;
      localStorage.setItem("sourceDb", sessionArray[index].sourceDbType);
      driver = sessionArray[index].driver;
      path = sessionArray[index].filePath;
      dbName = sessionArray[index].dbName;
      sourceDb = sessionArray[index].sourceDbType;
      pathArray = path.split("/");
      fileName = pathArray[pathArray.length - 1];
      filePath = "./" + fileName;
      readTextFile(filePath, async (error, text) => {
        if (error) {
          showSnackbar(err, " redBg");
        } else {
          let payload = {
            Driver: driver,
            DBName: dbName,
            FilePath: path,
          };
          localStorage.setItem("conversionReportContent", text);
          await Fetch.getAppData("POST", "/session/resume", payload);
        }
      });
      // return false;
    },
    switchToTab: (id) => {
      let others = ["report", "ddl", "summary"];
      others = others.filter((element) => element != id);
      tabbingHelper(id, others);
    },
    SearchTable: (value, tabId) => {
      console.log(value);
      let tableVal, list, listElem;
      let ShowResultNotFound = true;
      let schemaConversionObj = JSON.parse(
        localStorage.getItem("conversionReportContent")
      );
      if (tabId === "report") {
        list = document.getElementById(`accordion`);
      } else {
        list = document.getElementById(`${tabId}-accordion`);
      }
      listElem = list.getElementsByTagName("section");
      let tableListLength = Object.keys(schemaConversionObj.SpSchema).length;
      for (var i = 0; i < tableListLength; i++) {
        tableVal = Object.keys(schemaConversionObj.SpSchema)[i];
        if (tableVal.indexOf(value) > -1) {
          listElem[i].style.display = "";
          ShowResultNotFound = false;
        } else {
          listElem[i].style.display = "none";
        }
      }
      if (ShowResultNotFound) {
        list.style.display = "none";
        document.getElementById(`${tabId}notFound`).style.display = "block";
      } else {
        list.style.display = "";
        document.getElementById(`${tabId}notFound`).style.display = "none";
      }
    },
    expandAll: (text, buttonId) => {
      console.log(text, buttonId);
      let collapseSection = buttonId.substring(
        0,
        buttonId.indexOf("ExpandButton")
      );
      if (text === "Expand All") {
        document.getElementById(buttonId).innerHTML = "Collapse All";
        jQuery(`.${collapseSection}Collapse`).collapse("show");
      } else {
        document.getElementById(buttonId).innerHTML = "Expand All";
        jQuery(`.${collapseSection}Collapse`).collapse("hide");
      }
    },
    downloadSession: async () => {
      jQuery("<a />", {
        download: "session.json",
        href:
          "data:application/json;charset=utf-8," +
          encodeURIComponent(
            localStorage.getItem("conversionReportContent"),
            null,
            4
          ),
      })
        .appendTo("body")
        .click(function () {
          jQuery(this).remove();
        })[0]
        .click();
    },
    downloadDdl: async () => {
      let ddlreport = await Fetch.getAppData("GET", "/report");
      if (ddlreport.ok) {
        ddlreport.text().then(function (result) {
          localStorage.setItem("schemaFilePath", result);
        });

        let schemaFilePath = localStorage.getItem("schemaFilePath");
        let schemaFileName = schemaFilePath.split("/")[
          schemaFilePath.split("/").length - 1
        ];
        let filePath = "./" + schemaFileName;
        readTextFile(filePath, function (error, text) {
          jQuery("<a />", {
            download: schemaFileName,
            href:
              "data:application/json;charset=utf-8," + encodeURIComponent(text),
          })
            .appendTo("body")
            .click(function () {
              jQuery(this).remove();
            })[0]
            .click();
        });
      }
    },
    downloadReport: async () => {
      let summaryreport = await Fetch.getAppData("GET", "/report");
      console.log(summaryreport);
      if (summaryreport.ok) {
        await summaryreport.text().then(function (result) {
          localStorage.setItem("reportFilePath", result);
        });
        let reportFilePath = localStorage.getItem("reportFilePath");
        let reportFileName = reportFilePath.split("/")[
          reportFilePath.split("/").length - 1
        ];
        let filePath = "./" + reportFileName;
        readTextFile(filePath, function (error, text) {
          jQuery("<a />", {
            download: reportFileName,
            href:
              "data:application/json;charset=utf-8," + encodeURIComponent(text),
          })
            .appendTo("body")
            .click(function () {
              jQuery(this).remove();
            })[0]
            .click();
        });
      }
    },
    editGlobalDataType: () => {
      jQuery("#globalDataTypeModal").modal();
    },
    checkInterleaveConversion: async (tableName) => {
      let interleaveApiCall;
      interleaveApiCall = await Fetch.getAppData(
        "GET",
        "/setparent?table=" + tableName
      );
      return interleaveApiCall.json();
    },
    setGlobalDataType: async function () {
      let globalDataTypeList = JSON.parse(
        localStorage.getItem("globalDataTypeList")
      );
      let dataTypeListLength = Object.keys(globalDataTypeList).length;
      let dataTypeJson = {};
      for (var i = 0; i <= dataTypeListLength; i++) {
        var row = document.getElementById("dataTypeRow" + i);
        if (row) {
          var cells = row.getElementsByTagName("td");
          if (document.getElementById("dataTypeOption" + i) != null) {
            for (var j = 0; j < cells.length; j++) {
              if (j === 0) {
                var key = cells[j].innerText;
              } else {
                dataTypeJson[key] = document.getElementById(
                  "dataTypeOption" + i
                ).value;
              }
            }
          }
        }
      }
      await fetch("/typemap/global", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(dataTypeJson),
      }).then(async (res) => {
        console.log(res);
        res = await res.text();
        localStorage.setItem("conversionReportContent", res);
      });
    },
    getGlobalDataTypeList: async () => {

      let res = await Fetch.getAppData("GET", "/typemap");
     await res.json().then(function (result) {
        localStorage.setItem(
          "globalDataTypeList",
          JSON.stringify(result)
        );
      });
    },
    dataTypeUpdate: (id, globalDataTypeList) => {
      let selectedValue = document.getElementById(id).value;
      let idNum = parseInt(id.match(/\d+/), 10);
      let dataTypeOptionArray =
        globalDataTypeList[
          document.getElementById("dataTypeKey" + idNum).innerHTML
        ];
      console.log(dataTypeOptionArray);
      for (let i = 0; i < dataTypeOptionArray.length; i++) {
        if (dataTypeOptionArray[i].T === selectedValue) {
          if (dataTypeOptionArray[i].Brief !== "") {
            document.getElementById(`warning${idNum}`).style.display = "";
          } else {
            document.getElementById(`warning${idNum}`).style.display = "none";
          }
        }
      }
    },

    fetchIndexFormValues: async (name, uniqueness) => {
      if (keysList.length == 0) {
        showSnackbar(
          "Please select atleast one key to create a new index",
          " red-bg"
        );
        return;
      }
      let newIndex = {},
        newIndexPos;
      let jsonObj = JSON.parse(localStorage.getItem("conversionReportContent"));
      let table = jsonObj.SpSchema[srcTableName[tableNumber]];
      newIndex["Name"] = name;
      newIndex["Table"] = table.Name;
      if (uniqueness) {
        newIndex["Unique"] = true;
      } else {
        newIndex["Unique"] = false;
      }
      newIndex["Keys"] = keysList;
      if (table.Indexes != null && table.Indexes.length > 0) {
        newIndexPos = table.Indexes.length;
        for (let x = 0; x < table.Indexes.length; x++) {
          if (
            JSON.stringify(table.Indexes[x].Keys) === JSON.stringify(keysList)
          ) {
            showSnackbar(
              "Index with selected key(s) already exists.\n Please use different key(s)",
              " red-bg"
            );
            return;
          } else if (newIndex["Name"] === table.Indexes[x].Name) {
            showSnackbar(
              "Index with name: " +
                newIndex["Name"] +
                " already exists.\n Please try with a different name",
              " red-bg"
            );
            return;
          }
        }
      } else {
        newIndexPos = 0;
      }

      await fetch("/add/indexes?table=" + table.Name, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify([newIndex]),
      }).then(async function (res) {
        if (res.ok) {
          clearModal();
          jQuery("#createIndexModal").modal("hide");
          res = await res.text();
          localStorage.setItem("conversionReportContent", res);
          let jsonObj = JSON.parse(
            localStorage.getItem("conversionReportContent")
          );
          let table = jsonObj.SpSchema[srcTableName[tableNumber]];
          let indexKeys;
          jQuery("#" + tableNumber)
            .find(".index-acc-table.fk-table")
            .css("visibility", "visible");
          jQuery("#" + tableNumber)
            .find(".index-acc-table.fk-table")
            .addClass("important-rule-100");
          jQuery("#" + tableNumber)
            .find(".index-acc-table.fk-table")
            .removeClass("important-rule-0");
          $indexTableContent = jQuery(".indexTableTr.template")
            .clone()
            .removeClass("template");
          $indexTableContent
            .find(".renameSecIndex.template")
            .attr("id", "renameSecIndex" + tableNumber + newIndexPos);
          $indexTableContent
            .find(".saveSecIndex.template")
            .attr("id", "saveSecIndex" + tableNumber + newIndexPos);
          if (
            document
              .getElementById("editSpanner" + tableNumber)
              .innerHTML.trim() == "Save Changes"
          ) {
            $indexTableContent
              .find(".renameSecIndex.template")
              .removeClass("template")
              .find("input")
              .val(table.Indexes[newIndexPos].Name)
              .attr("id", "newSecIndexVal" + tableNumber + newIndexPos);
            $indexTableContent.find("button").removeAttr("disabled");
          } else {
            $indexTableContent
              .find(".saveSecIndex.template")
              .removeClass("template")
              .html(table.Indexes[newIndexPos].Name);
          }
          $indexTableContent
            .find(".acc-table-td.indexesTable")
            .html(table.Indexes[newIndexPos].Table);
          $indexTableContent
            .find(".acc-table-td.indexesUnique")
            .html(table.Indexes[newIndexPos].Unique.toString());
          indexKeys = "";
          for (var k = 0; k < table.Indexes[newIndexPos].Keys.length; k++) {
            indexKeys += table.Indexes[newIndexPos].Keys[k].Col + ", ";
          }
          indexKeys = indexKeys.replace(/,\s*$/, "");
          $indexTableContent.find(".acc-table-td.indexesKeys").html(indexKeys);
          $indexTableContent
            .find("button")
            .attr("id", table.Name + newIndexPos + "secIndex");
          $indexTableContent
            .find("#" + table.Name + newIndexPos + "secIndex")
            .click(function () {
              let indexId = jQuery(this).attr("id");
              let secIndexTableNumber = parseInt(
                jQuery(this)
                  .closest(".index-collapse.collapse")
                  .attr("id")
                  .match(/\d+/),
                10
              );
              localStorage.setItem("indexId", indexId);
              localStorage.setItem("secIndexTableNumber", secIndexTableNumber);
              jQuery("#secIndexDeleteWarning").modal();
            });
          $indexTableContent.appendTo(
            jQuery("#" + tableNumber).find(".indexTableBody")
          );
        } else {
          res = await res.text();
          showSnackbar(res, " red-bg");
        }
      });
    },
    createNewSecIndex :(id)=>{
      console.log(id.substring(12));
      let generalModal = document.getElementsByTagName('hb-modal')[1]
      let content = `<hb-add-index-form tableName="${id.substring(12)}"></hb-add-index-form>`
      generalModal.setAttribute('content',content )
      console.log(generalModal);
      jQuery("#createIndexModal").modal();
       keysList = [];
       orderId = 0;
      TEMP = {}
    },
    changeCheckBox:(row ,id)=>{
      
      let columnName = document.getElementById(`order${row}${id}`);
      let checkboxValue = document.getElementById('checkbox-'+row+"-"+id).checked 
      if(checkboxValue)
      {
        columnName.style.visibility=""
        columnName.innerHTML = orderId+1;
        orderId++;
        keysList.push({Col:row,Desc:false})
        TEMP[row] = id;
      }
      else{
        columnName.style.visibility="hidden"
        let oldValue = parseInt(columnName.innerHTML);
      
        for(let i=0;i<keysList.length;i++)
        {
          let currentRow = keysList[i].Col
          let currentId = TEMP[currentRow]
          let currentColName = document.getElementById(`order${currentRow}${currentId}`)
          console.log(currentColName);
          if(parseInt(currentColName.innerHTML) > oldValue)
          {
            currentColName.innerHTML = parseInt(currentColName.innerHTML) - 1;
          }
        }
        keysList = keysList.filter((cur) => cur.Col !== row )
        TEMP[row] = -1;
        console.log(keysList , TEMP);
        orderId--;
      }
     
     
    }
    
  };
})();

export default Actions;
