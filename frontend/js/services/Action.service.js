import Store from "./Store.service.js";
import Fetch from "./Fetch.service.js";
import {
  readTextFile,
  createEditDataTypeTable,
  showSnackbar,
  tabbingHelper
} from "./../helpers/SchemaConversionHelper.js";
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
      others = others.filter((element)=>element!=id);
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
      let ddlreport = await Fetch.getAppData('GET', '/report');
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
           let summaryreport = await Fetch.getAppData('GET', '/report');
           console.log(summaryreport);
           if (summaryreport.ok) {
                await summaryreport.text().then(function (result) {
                localStorage.setItem('reportFilePath', result);
              });
              let reportFilePath = localStorage.getItem('reportFilePath');
              let reportFileName = reportFilePath.split('/')[reportFilePath.split('/').length - 1];
              let filePath = './' + reportFileName;
              readTextFile(filePath, function (error, text) {
                  jQuery("<a />", {
                      "download": reportFileName,
                      "href": "data:application/json;charset=utf-8," + encodeURIComponent(text),
                  }).appendTo("body")
                      .click(function () {
                          jQuery(this).remove()
                      })[0].click();
              })
           }    
        },
        editGlobalDataType: () => {  
            createEditDataTypeTable();
            jQuery('#globalDataTypeModal').modal();
        },
        checkInterleaveConversion: async (tableName) => {
            let interleaveApiCall;
            interleaveApiCall = await Fetch.getAppData('GET', '/setparent?table='+tableName);
            return interleaveApiCall.json();
          },
          setGlobalDataType: async function(){
            let globalDataTypeList = JSON.parse(localStorage.getItem('globalDataTypeList'));
            let dataTypeListLength = Object.keys(globalDataTypeList).length;
            let dataTypeJson = {};
            for (var i = 0; i <= dataTypeListLength; i++) {
              var row = document.getElementById('dataTypeRow' + i);
              if (row) {
                var cells = row.getElementsByTagName('td');
                if (document.getElementById('dataTypeOption' + i) != null) {
                  for (var j = 0; j < cells.length; j++) {
                    if (j === 0) {
                      var key = cells[j].innerText;
                    }
                    else {
                      dataTypeJson[key] = document.getElementById('dataTypeOption' + i).value;
                    }
                  }
                }
              }
            }
            await fetch('/typemap/global', {
              method: 'POST',
              headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
              },
              body: JSON.stringify(dataTypeJson)
            })
              .then(async (res)=> {
                console.log(res);
                res = await res.text();
                localStorage.setItem('conversionReportContent', res);
               
              })
            },
          getGlobalDataTypeList: async () => {
             await fetch('/typemap', {
                method: 'GET',
                headers: {
                  'Accept': 'application/json',
                  'Content-Type': 'application/json'
                }
              })
                .then(function (res) {
                  if (res.ok) {
                    res.json().then(function (result) {
                      localStorage.setItem('globalDataTypeList', JSON.stringify(result));
                    });
                  }
                  else {
                    return Promise.reject(res);
                  }
                }).catch(function (err) {
                  showSnackbar(err, ' redBg');
                });
            }
          }
})();

export default Actions;
