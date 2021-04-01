import Store from "./Store.service.js";
import Fetch from "./Fetch.service.js";
import { readTextFile } from "../helpers/SchemaConversionHelper.js";

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
        onLoadDatabase: async (dbType, dumpFilePath) => {
            let reportData, sourceTableFlag, reportDataResp, reportDataCopy, jsonReportDataResp, requestCode;
            reportData = await Fetch.getAppData('POST', '/convert/dump', { "Driver": dbType, "Path": dumpFilePath });
            reportDataCopy = reportData.clone();
            requestCode = reportData.status;
            reportDataResp = await reportData.text();
            if (requestCode != 200) {
                Fetch.showSnackbar(reportDataResp, ' redBg');
                return false;
            }
            else {
                jsonReportDataResp = await reportDataCopy.json();
                console.log(jsonReportDataResp);
                if (Object.keys(jsonReportDataResp.SpSchema).length == 0) {
                    showSnackbar("Please select valid file", " redBg");
                    return false;
                }
                else {
                    // showSpinner();
                    jQuery('#loadDatabaseDumpModal').modal('hide');
                    localStorage.setItem('conversionReportContent', reportDataResp);
                }
            }
            sourceTableFlag = localStorage.getItem('sourceDbName');
            // sessionRetrieval(sourceTableFlag);
            return true;
        },
        onconnect: async (dbType, dbHost, dbPort, dbUser, dbName, dbPassword) => {
            let sourceTableFlag = '', response;
            let payload = {
                "Driver": dbType,
                "Database": dbName,
                "Password": dbPassword,
                "User": dbUser,
                "Port": dbPort,
                "Host": dbHost
            };
            response = await Fetch.getAppData('POST', '/connect', payload);
            if (response.ok) {
                if (dbType === 'mysql')
                    sourceTableFlag = 'MySQL';
                else if (dbType === 'postgres')
                    sourceTableFlag = 'Postgres';
                localStorage.setItem('sourceDbName', sourceTableFlag);
                jQuery('#connectToDbModal').modal('hide');
                jQuery('#connectModalSuccess').modal();
            }
            else {
                jQuery('#connectToDbModal').modal('hide');
                jQuery('#connectModalFailure').modal();
            }
            return response;
        },
        showSchemaAssessment: async () => {
            let reportDataResp, reportData, sourceTableFlag;
            reportData = await Fetch.getAppData('GET', '/convert/infoschema');
            reportDataResp = await reportData.text();
            localStorage.setItem('conversionReportContent', reportDataResp);
            jQuery('#connectModalSuccess').modal("hide");
            sourceTableFlag = localStorage.getItem('sourceDbName');
        },
        ddlSummaryAndConversionApiCall: async () => {
            let conversionRate, conversionRateJson, ddlData, ddlDataJson, summaryData, summaryDataJson;
            ddlData = await Fetch.getAppData('GET', '/ddl');
            summaryData = await Fetch.getAppData('GET', '/summary');
            conversionRate = await Fetch.getAppData('GET', '/conversion');
            if (ddlData.ok && summaryData.ok && conversionRate.ok) {
                ddlDataJson = await ddlData.json();
                summaryDataJson = await summaryData.json();
                conversionRateJson = await conversionRate.json();
                localStorage.setItem('ddlStatementsContent', JSON.stringify(ddlDataJson));
                localStorage.setItem('summaryReportContent', JSON.stringify(summaryDataJson));
                localStorage.setItem('tableBorderColor', JSON.stringify(conversionRateJson));
            }
            else {
                return false;
            }
            return true;
        },


        // addNewSession: (session) => {
        //     Store.addNewSession(session);
        // },
        // resumeSession: (index) => {
        //     let val = Store.getSessionData(index);
        //     console.log(val);
        // },
        // getAllSessions: () => {
        // return Store.getAllSessions();},

        sessionRetrieval: async (dbType) => {
            let sessionStorageArr, sessionInfo, sessionResp;
            sessionResp = await Fetch.getAppData('GET', '/session');
            sessionInfo = await sessionResp.json();
            sessionStorageArr = JSON.parse(sessionStorage.getItem('sessionStorage'));
            if (sessionStorageArr == undefined)
                sessionStorageArr = [];
            sessionInfo.sourceDbType = dbType;
            sessionStorageArr.unshift(sessionInfo);
            sessionStorage.setItem('sessionStorage', JSON.stringify(sessionStorageArr));
        },
        resumeSessionHandler: async (index, sessionArray) => {
            let driver, path, dbName, sourceDb, pathArray, fileName, filePath;
            localStorage.setItem('sourceDb', sessionArray[index].sourceDbType);
            driver = sessionArray[index].driver;
            path = sessionArray[index].filePath;
            dbName = sessionArray[index].dbName;
            sourceDb = sessionArray[index].sourceDbType;
            pathArray = path.split('/');
            fileName = pathArray[pathArray.length - 1];
            filePath = './' + fileName;
            readTextFile(filePath, async (error, text) => {
                if (error) {
                    showSnackbar(err, ' redBg');
                }
                else {
                    let payload = {
                        "Driver": driver,
                        "DBName": dbName,
                        "FilePath": path
                    }
                    localStorage.setItem('conversionReportContent', text);
                    await Fetch.getAppData('POST', '/session/resume', payload);
                }
            });
            // return false;
        },
        switchToTab: (id) => {
            Store.changeCurrentTab(id)
        },
        SearchTable: (value, tabId) => {
            console.log(value);
            let tableVal, list, listElem;
            let ShowResultNotFound = true;
            let schemaConversionObj = JSON.parse(
                localStorage.getItem("conversionReportContent")
            );
            if (tabId === "reportTab") {
                list = document.getElementById("reportDiv");
            } else if (tabId === "ddlTab") {
                list = document.getElementById("ddlDiv");
            } else {
                list = document.getElementById("summaryDiv");
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
                document.getElementById("notFound").style.display = "block";
            } else {
                document.getElementById("notFound").style.display = "none";
            }
        },
    };
})();

export default Actions;
