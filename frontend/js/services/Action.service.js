import Store from "./Store.service.js";
import Fetch from "./Fetch.service.js";
import { readTextFile, showSnackbar, tabbingHelper, recreateNode } from "./../helpers/SchemaConversionHelper.js";

var keysList = [];
var orderId = 0;
var temp = {};
/**
 * All the manipulations to the store happen via the actions mentioned in this module
 *
 */

const resetIndexModal = () => {
  keysList = [];
  orderId = 0;
  temp = {};
}

const Actions = (() => {
  return {
    trial: () => {
      return "1";
    },
    addAttrToStore: () => {
      Store.addAttrToStore();
    },
    closeStore: () => {
      Store.toggleStore();
    },
    onLoadDatabase: async (dbType, dumpFilePath) => {
      let reportData, sourceTableFlag, reportDataResp, reportDataCopy, jsonReportDataResp, requestCode;
      reportData = await Fetch.getAppData("POST", "/convert/dump", { Driver: dbType, Path: dumpFilePath });
      reportDataCopy = reportData.clone();
      requestCode = reportData.status;
      reportDataResp = await reportData.text();
      if (requestCode != 200) {
        showSnackbar(reportDataResp, " redBg");
        return false;
      }
      else {
        jsonReportDataResp = await reportDataCopy.json();
        if (Object.keys(jsonReportDataResp.SpSchema).length == 0) {
          showSnackbar("Please select valid file", " redBg");
          return false;
        } else {
          jQuery("#loadDatabaseDumpModal").modal("hide");
          localStorage.setItem("conversionReportContent", reportDataResp);
        }
      }
      sourceTableFlag = localStorage.getItem("sourceDbName");
      return true;
    },
    onconnect: async (dbType, dbHost, dbPort, dbUser, dbName, dbPassword) => {
      let sourceTableFlag = "", response;
      let payload = { Driver: dbType, Database: dbName, Password: dbPassword, User: dbUser, Port: dbPort, Host: dbHost };
      response = await Fetch.getAppData("POST", "/connect", payload);
      if (response.ok) {
        if (dbType === "mysql") sourceTableFlag = "MySQL";
        else if (dbType === "postgres") sourceTableFlag = "Postgres";
        localStorage.setItem("sourceDbName", sourceTableFlag);
        jQuery("#connectToDbModal").modal("hide");
        jQuery("#connectModalSuccess").modal();
      }
      else {
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
    onLoadSessionFile: async (filePath) => {
      let driver = '', response, payload;
      let srcDb = localStorage.getItem('sourceDbName');
      if (srcDb === 'MySQL') {
        driver = 'mysqldump';
      }
      else if (srcDb === 'Postgres') {
        driver = 'pg_dump';
      }
      payload = { "Driver": driver, "DBName": '', "FilePath": filePath };
      response = await Fetch.getAppData('POST', '/session/resume', payload);
      if (response.ok) {
        let responseCopy, textResponse, jsonResponse;
        responseCopy = response.clone();
        textResponse = await response.text();
        jsonResponse = await responseCopy.json();
        if (Object.keys(jsonResponse.SpSchema).length == 0) {
          showSnackbar('Please select valid session file', ' redBg');
          jQuery('#importButton').attr('disabled', 'disabled');
          return false;
        }
        else {
          localStorage.setItem('conversionReportContent', textResponse);
          jQuery('#loadSchemaModal').modal('hide');
          return true;
        }
      }
      else {
        showSnackbar('Please select valid session file', ' redBg');
        jQuery('#importButton').attr('disabled', 'disabled');
        return false;
      }
    },
    ddlSummaryAndConversionApiCall: async () => {
      let conversionRate, conversionRateJson, ddlData, ddlDataJson, summaryData, summaryDataJson;
      ddlData = await Fetch.getAppData("GET", "/ddl");
      summaryData = await Fetch.getAppData("GET", "/summary");
      conversionRate = await Fetch.getAppData("GET", "/conversion");
      if (ddlData.ok && summaryData.ok && conversionRate.ok) {
        ddlDataJson = await ddlData.json();
        summaryDataJson = await summaryData.json();
        conversionRateJson = await conversionRate.json();
        localStorage.setItem("ddlStatementsContent", JSON.stringify(ddlDataJson));
        localStorage.setItem("summaryReportContent", JSON.stringify(summaryDataJson));
        localStorage.setItem("tableBorderColor", JSON.stringify(conversionRateJson));
      }
      else {
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
      sessionStorage.setItem("sessionStorage", JSON.stringify(sessionStorageArr));
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
        }
        else {
          let payload = {Driver: driver, DBName: dbName, FilePath: path};
          localStorage.setItem("conversionReportContent", text);
          await Fetch.getAppData("POST", "/session/resume", payload);
        }
      });
    },
    switchToTab: (id) => {
      let others = ["report", "ddl", "summary"];
      others = others.filter((element) => element != id);
      tabbingHelper(id, others);
    },
    SearchTable: (value, tabId) => {
      let tableVal, list, listElem;
      let ShowResultNotFound = true;
      let schemaConversionObj = JSON.parse(localStorage.getItem("conversionReportContent"));
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
        }
        else {
          listElem[i].style.display = "none";
        }
      }
      if (ShowResultNotFound) {
        list.style.display = "none";
        document.getElementById(`${tabId}notFound`).style.display = "block";
      }
      else {
        list.style.display = "";
        document.getElementById(`${tabId}notFound`).style.display = "none";
      }
    },
    expandAll: (text, buttonId) => {
      let collapseSection = buttonId.substring(0, buttonId.indexOf("ExpandButton"));
      if (text === "Expand All") {
        document.getElementById(buttonId).innerHTML = "Collapse All";
        jQuery(`.${collapseSection}Collapse`).collapse("show");
      }
      else {
        document.getElementById(buttonId).innerHTML = "Expand All";
        jQuery(`.${collapseSection}Collapse`).collapse("hide");
      }
    },
    downloadSession: async () => {
      jQuery("<a />", {
        download: "session.json",
        href: "data:application/json;charset=utf-8," + encodeURIComponent(localStorage.getItem("conversionReportContent"), null, 4),
      })
      .appendTo("body")
      .click(function () {
        jQuery(this).remove();
      })[0]
      .click();
    },
    downloadDdl: async () => {
      let ddlreport = await Fetch.getAppData("GET", "/schema");
      if (ddlreport.ok) {
        await ddlreport.text().then(function (result) {
          localStorage.setItem("schemaFilePath", result);
        });
        let schemaFilePath = localStorage.getItem("schemaFilePath");
        if (schemaFilePath) {
          let schemaFileName = schemaFilePath.split("/")[schemaFilePath.split("/").length - 1];
          let filePath = "./" + schemaFileName;
          readTextFile(filePath, function (error, text) {
            jQuery("<a />", {
              download: schemaFileName,
              href: "data:application/json;charset=utf-8," + encodeURIComponent(text),
            })
            .appendTo("body")
            .click(function () {
              jQuery(this).remove();
            })[0]
            .click();
          });
        }
        showSnackbar('try again ', 'red')
      }
    },
    downloadReport: async () => {
      let summaryreport = await Fetch.getAppData("GET", "/report");
      if (summaryreport.ok) {
        await summaryreport.text().then(function (result) {
          localStorage.setItem("reportFilePath", result);
        });
        let reportFilePath = localStorage.getItem("reportFilePath");
        let reportFileName = reportFilePath.split("/")[reportFilePath.split("/").length - 1];
        let filePath = "./" + reportFileName;
        readTextFile(filePath, function (error, text) {
          jQuery("<a />", {
            download: reportFileName,
            href: "data:application/json;charset=utf-8," + encodeURIComponent(text),
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
      interleaveApiCall = await Fetch.getAppData("GET", "/setparent?table=" + tableName);
      let interleaveApiCallResp = await interleaveApiCall.json();
      let value = interleaveApiCallResp.tableInterleaveStatus.Possible;
      Store.setInterleave(tableName,value);
    },
    setGlobalDataType: async () => {
      let globalDataTypeList = JSON.parse(localStorage.getItem("globalDataTypeList"));
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
              }
              else {
                dataTypeJson[key] = document.getElementById(
                  "dataTypeOption" + i
                ).value;
              }
            }
          }
        }
      }
      let res = await Fetch.getAppData("POST", "/typemap/global", dataTypeJson);
      if (res) {
        res = await res.text();
        localStorage.setItem("conversionReportContent", res);
      }
    },
    getGlobalDataTypeList: async () => {
      let res = await Fetch.getAppData("GET", "/typemap");
      await res.json().then(function (result) {
        localStorage.setItem("globalDataTypeList", JSON.stringify(result));
      });
    },
    dataTypeUpdate: (id, globalDataTypeList) => {
      let selectedValue = document.getElementById(id).value;
      let idNum = parseInt(id.match(/\d+/), 10);
      let dataTypeOptionArray = globalDataTypeList[document.getElementById("dataTypeKey" + idNum).innerHTML];
      for (let i = 0; i < dataTypeOptionArray.length; i++) {
        if (dataTypeOptionArray[i].T === selectedValue) {
          if (dataTypeOptionArray[i].Brief !== "") {
            document.getElementById(`warning${idNum}`).style.display = "";
          }
          else {
            document.getElementById(`warning${idNum}`).style.display = "none";
          }
        }
      }
    },
    fetchIndexFormValues: async function (tableIndex, tableName, name, uniqueness) {
      if (keysList.length === 0) {
        showSnackbar("Please select atleast one key to create a new index", " redBg");
        return;
      }
      let newIndex = {};
      let newIndexPos = 1;
      let jsonObj = JSON.parse(localStorage.getItem("conversionReportContent"));
      let table = jsonObj.SpSchema[tableName];
      newIndex["Name"] = name;
      newIndex["Table"] = table.Name;
      if (uniqueness) {
        newIndex["Unique"] = true;
      }
      else {
        newIndex["Unique"] = false;
      }
      newIndex["Keys"] = keysList;
      if (table.Indexes != null && table.Indexes.length > 0) {
        newIndexPos = table.Indexes.length;
        for (let x = 0; x < table.Indexes.length; x++) {
          if (JSON.stringify(table.Indexes[x].Keys) === JSON.stringify(keysList)) {
            showSnackbar("Index with selected key(s) already exists.\n Please use different key(s)", " redBg");
            return;
          }
          else if (newIndex["Name"] === table.Indexes[x].Name) {
            showSnackbar("Index with name: " + newIndex["Name"] + " already exists.\n Please try with a different name", " redBg");
            return;
          }
        }
      }
      else {
        newIndexPos = 0;
      }
      let res = await Fetch.getAppData("POST", "/add/indexes?table=" + tableName, [newIndex]);
      if (res.ok) {
        jQuery("#createIndexModal").modal("hide");
        res = await res.text();
        localStorage.setItem("conversionReportContent", res);
        jsonObj = JSON.parse(localStorage.getItem("conversionReportContent"));
        let secIndexArray = jsonObj.SpSchema[tableName].Indexes;
        let tablemap = document.querySelector("#indexKey" + tableIndex).querySelector(".index-acc-table.fkTable");
        let flag = document.querySelector("#editSpanner" + tableIndex).innerHTML === "Save Changes";
        tablemap.innerHTML = ` ${secIndexArray && secIndexArray.length > 0 ? 
        `<thead>
          <tr>
              <th>Name</th>
              <th>Table</th>
              <th>Unique</th>
              <th>Keys</th>
              <th>Action</th>
          </tr>
        </thead>`: 
        `<div></div>`} 
        ${secIndexArray.map((secIndex, index) => {
          return `
            <tr class="indexTableTr ">
                <td class="acc-table-td indexesName">
                    <div class="renameSecIndex ${flag ? '' : 'template'}" id="renameSecIndex${tableIndex}${index}">
                        <input type="text" id="newSecIndexVal${tableIndex}${index}" value="${secIndex.Name}"
                            class="form-control spanner-input" autocomplete="off" />
                    </div>
                    <div class="saveSecIndex ${flag ? 'template' : ''}" id="saveSecIndex${tableIndex}${index}">${secIndex.Name}</div>
                </td>
                <td class="acc-table-td indexesTable">${secIndex.Table}</td>
                <td class="acc-table-td indexesUnique">${secIndex.Unique}</td>
                <td class="acc-table-td indexesKeys">${secIndex.Keys.map((key) => key.Col).join(',')}</td>
                <td class="acc-table-td indexesAction">
                    <button class="dropButton" id="${tableName}${index}secIndex" ${flag ? "" : "disabled"}>
                        <span><i class="large material-icons removeIcon"
                                style="vertical-align: middle">delete</i></span>
                        <span style="vertical-align: middle">Drop</span>
                    </button>
                </td>
            </tr>
          `;
        }).join("")}`;
  
        if (secIndexArray !== null && secIndexArray.length > 0) {
          secIndexArray.map((secIndex, index) => {
            document.getElementById(tableName + index + 'secIndex').addEventListener('click', () => {
              jQuery('#indexAndKeyDeleteWarning').modal();
              jQuery('#indexAndKeyDeleteWarning').find('#modal-content').html(`This will permanently delete the secondary index and the corresponding uniqueness constraints on
                indexed columns (if applicable). Do you want to continue?`);
              recreateNode(document.getElementById('fk-drop-confirm'))
              document.getElementById('fk-drop-confirm').addEventListener('click', () => {
                Actions.dropSecondaryIndexHandler(tableName, tableIndex, index);
              })
            })
          });
        }
        this.closeSecIndexModal();
      }
    },
    createNewSecIndex: (id) => {
      let iIndex = id.indexOf("indexButton");
      let tableIndex = id.substring(0, iIndex)
      let tableName = id.substring(iIndex + 12)
      let jsonObj = JSON.parse(localStorage.getItem('conversionReportContent'));
      if (document.getElementById("editSpanner" + tableIndex).innerHTML.trim() == "Save Changes") {
        let pendingChanges = false;
        let dataTable = jQuery(`#src-sp-table${tableIndex} tr`)
        dataTable.each(function (index) {
          if (index > 1) {
            let newColumnName;
            let srcColumnName = jQuery(this).find('.srcColumn').html().trim();
            let indexNumber = jQuery(this).find('.srcColumn').attr('id').match(/\d+/)[0];
            indexNumber = indexNumber.substring(tableIndex.toString().length);
            let indexNumberlength = indexNumber.length / 2;
            indexNumber = indexNumber.substring(indexNumberlength);
            let newColumnNameEle = document.getElementById('columnNameText' + tableIndex + indexNumber + indexNumber);
            if (newColumnNameEle) {
              newColumnName = newColumnNameEle.value;
            }
            let oldColumnName = jsonObj.ToSpanner[tableName].Cols[srcColumnName];
            if (newColumnName !== oldColumnName || !(jQuery(this).find("input[type=checkbox]").is(":checked"))) {
              let errorModal = document.querySelector("hb-modal[modalId = editTableWarningModal]");
              let content = "There are pending changes to this table, please save the same before creating the index";
              errorModal.setAttribute("content", content);
              jQuery("#editTableWarningModal").modal();
              pendingChanges = true;
            }
          }
        })
        if (pendingChanges) {
          return;
        }
      }
      let generalModal = document.querySelector("hb-modal[modalId = createIndexModal]")
      let content = `<hb-add-index-form tableName=${tableName} tableIndex=${tableIndex}></hb-add-index-form>`;
      generalModal.setAttribute("content", content);
      jQuery("#createIndexModal").modal();
      resetIndexModal();
    },
    closeSecIndexModal: () => {
      resetIndexModal();
      let generalModal = document.querySelector("hb-modal[modalId = createIndexModal]");
      let content = `empty`;
      generalModal.setAttribute("content", content);
    },
    changeCheckBox: (row, id) => {
      let columnName = document.getElementById(`order${row}${id}`);
      let checkboxValue = document.getElementById("checkbox-" + row + "-" + id).checked;
      if (checkboxValue) {
        columnName.style.visibility = "visible";
        columnName.innerHTML = orderId + 1;
        orderId++;
        keysList.push({ Col: row, Desc: false });
        temp[row] = id;
      }
      else {
        columnName.style.visibility = "hidden";
        let oldValue = parseInt(columnName.innerHTML);
        for (let i = 0; i < keysList.length; i++) {
          let currentRow = keysList[i].Col;
          let currentId = temp[currentRow];
          let currentColName = document.getElementById(`order${currentRow}${currentId}`);
          if (parseInt(currentColName.innerHTML) > oldValue) {
            currentColName.innerHTML = parseInt(currentColName.innerHTML) - 1;
          }
        }
        keysList = keysList.filter((cur) => cur.Col !== row);
        temp[row] = -1;
        orderId--;
      }
    },
    editAndSaveButtonHandler: async (event, tableNumber, tableName, notNullConstraint) => {
      let schemaConversionObj = JSON.parse(localStorage.getItem("conversionReportContent"));
      let tableId = '#src-sp-table' + tableNumber + ' tr';
      let tableColumnNumber = 0, tableData, fkTableData, secIndexTableData;
      let renameFkMap = {}, fkLength, secIndexLength, renameIndexMap = {};
      if (event.target.innerHTML.trim() === "Edit Spanner Schema") {
        let uncheckCount = [], $selectAll, $selectEachRow, checkAllTableNumber, checkClassTableNumber, spannerCellsList;
        let tableCheckboxGroup = '.chckClass_' + tableNumber;
        uncheckCount[tableNumber] = 0;
        event.target.innerHTML = "Save Changes";
        document.getElementById("editInstruction" + tableNumber).style.visibility = "hidden";
        jQuery(tableId).each(function (index) {
          if (index === 1) {
            $selectAll = jQuery(this).find('.bmd-form-group.is-filled.template').removeClass('template');
          }
          checkAllTableNumber = jQuery('#chckAll_' + tableNumber);
          checkAllTableNumber.prop('checked', true);
          checkAllTableNumber.click(function () {
            tableNumber = parseInt(jQuery(this).attr('id').match(/\d+/), 10);
            checkClassTableNumber = jQuery('.chckClass_' + tableNumber);
            switch (jQuery(this).is(':checked')) {
              case true:
                checkClassTableNumber.prop('checked', true);
                uncheckCount[tableNumber] = 0;
                break;
              case false:
                checkClassTableNumber.prop('checked', false);
                uncheckCount[tableNumber] = Object.keys(schemaConversionObj.ToSpanner[schemaConversionObj.SpSchema[tableName].Name].Cols).length;
                break;
            }
          });
          if (index > 1) {
            $selectEachRow = jQuery(this).find('.bmd-form-group.is-filled.eachRowChckBox.template').removeClass('template');
            jQuery(tableCheckboxGroup).prop('checked', true);
            spannerCellsList = document.getElementsByClassName('spannerTabCell' + tableNumber + tableColumnNumber);
            if (spannerCellsList) {
              // edit column name
              jQuery('#editColumnName' + tableNumber + tableColumnNumber).removeClass('template');
              jQuery('#saveColumnName' + tableNumber + tableColumnNumber).addClass('template');
              // edit data type
              jQuery('#editDataType' + tableNumber + tableColumnNumber).removeClass('template');
              jQuery('#saveDataType' + tableNumber + tableColumnNumber).addClass('template');
              let dataTypeArray = null;
              let globalDataTypes = JSON.parse(localStorage.getItem('globalDataTypeList'));
              let globalDataTypesLength = Object.keys(globalDataTypes).length;
              let srcCellValue = document.getElementById('srcDataType' + tableNumber + tableColumnNumber).innerHTML;
              let spannerCellValue = document.getElementById('saveDataType' + tableNumber + tableColumnNumber).innerHTML;
              let options = '';
              for (let a = 0; a < globalDataTypesLength; a++) {
                if (srcCellValue.trim().toLowerCase() === (Object.keys(globalDataTypes)[a]).toLowerCase()) {
                  dataTypeArray = globalDataTypes[Object.keys(globalDataTypes)[a]];
                  break;
                }
              }
              if (dataTypeArray !== null) {
                let dataTypeArrayLength = dataTypeArray.length;
                for (let a = 0; a < dataTypeArrayLength; a++) {
                  if (spannerCellValue.trim() == dataTypeArray[a].T) {
                    options += '<option class="dataTypeOption" value=' + dataTypeArray[a].T + ' selected>' + dataTypeArray[a].T + '</option>';
                  }
                  else {
                    options += '<option class="dataTypeOption" value=' + dataTypeArray[a].T + '>' + dataTypeArray[a].T + '</option>';
                  }
                }
              }
              else {
                options += '<option class="dataTypeOption" value=' + spannerCellValue + '>' + spannerCellValue + '</option>';
              }
              document.getElementById("dataType" + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML = options;
              // edit constraint
              let notNullFound = '';
              let constraintId = 'spConstraint' + tableNumber + tableColumnNumber;
              let columnName = jQuery('#saveColumnName' + tableNumber + tableColumnNumber).find('.column.right.spannerColNameSpan').html();
              if (schemaConversionObj.SpSchema[tableName].ColDefs[columnName].NotNull === true) {
                notNullFound = "<option class='active' selected>Not Null</option>";
              }
              else if (schemaConversionObj.SpSchema[tableName].ColDefs[columnName].NotNull === false) {
                notNullFound = "<option>Not Null</option>";
              }
              let constraintHtml = "<select id=" + constraintId + " multiple size='0' class='form-control spanner-input tableSelect' >"
                + notNullFound
                + "</select>";
              spannerCellsList[2].innerHTML = constraintHtml;
              new vanillaSelectBox("#spConstraint" + tableNumber + tableColumnNumber, {
                placeHolder: "Select Constraints",
                maxWidth: 500,
                maxHeight: 300
              });
              jQuery('#spConstraint' + tableNumber + tableColumnNumber).on('change', function () {
                let idNum = parseInt(jQuery(this).attr('id').match(/\d+/g), 10);
                let constraints = document.getElementById(constraintId);
                if (constraints) {
                  let constraintsLength = constraints.length;
                  for (let c = 0; c < constraintsLength; c++) {
                    if (constraints.options[c].selected) {
                      notNullConstraint[idNum] = 'Not Null';
                    }
                    else {
                      notNullConstraint[idNum] = '';
                    }
                  }
                }
              });
            }
            tableColumnNumber++;
          }
        });
        checkClassTableNumber = jQuery('.chckClass_' + tableNumber);
        checkClassTableNumber.click(function () {
          tableNumber = parseInt(jQuery(this).closest("table").attr('id').match(/\d+/), 10);
          checkAllTableNumber = jQuery('#chckAll_' + tableNumber);
          if (jQuery(this).is(":checked")) {
            uncheckCount[tableNumber] = uncheckCount[tableNumber] - 1;
            if (uncheckCount[tableNumber] === 0) {
              checkAllTableNumber.prop('checked', true);
            }
          }
          else {
            uncheckCount[tableNumber] = uncheckCount[tableNumber] + 1;
            checkAllTableNumber.prop('checked', false);
          }
        });
        if (schemaConversionObj.SpSchema[tableName].Fks != null && schemaConversionObj.SpSchema[tableName].Fks.length != 0) {
          fkLength = schemaConversionObj.SpSchema[tableName].Fks.length;
          for (let x = 0; x < fkLength; x++) {
            jQuery('#renameFk' + tableNumber + x).removeClass('template');
            jQuery('#saveFk' + tableNumber + x).addClass('template');
          }
          if (schemaConversionObj.SpSchema[tableName].Fks != null && schemaConversionObj.SpSchema[tableName].Fks.length != 0) {
            for (let p = 0; p < schemaConversionObj.SpSchema[tableName].Fks.length; p++) {
              jQuery("#" + tableName + p + 'foreignKey').removeAttr('disabled');
            }
          }
        }
        if (schemaConversionObj.SpSchema[tableName].Indexes != null && schemaConversionObj.SpSchema[tableName].Indexes.length != 0) {
          secIndexLength = schemaConversionObj.SpSchema[tableName].Indexes.length;
          for (let x = 0; x < secIndexLength; x++) {
            jQuery('#renameSecIndex' + tableNumber + x).removeClass('template');
            jQuery('#saveSecIndex' + tableNumber + x).addClass('template');
          }
          if (schemaConversionObj.SpSchema[tableName].Indexes != null && schemaConversionObj.SpSchema[tableName].Indexes.length != 0) {
            for (let p = 0; p < schemaConversionObj.SpSchema[tableName].Indexes.length; p++) {
              jQuery("#" + tableName + p + 'secIndex').removeAttr('disabled');
            }
          }
        }
      }
      else if (event.target.innerHTML.trim() === "Save Changes") {
        let columnNameExists = false, changesSuccess = false;
        let updatedColsData = {
          'UpdateCols': {
          }
        }
        jQuery(tableId).each(function (index) {
          if (index > 1) {
            let newColumnName;
            let srcColumnName = document.getElementById('srcColumnName' + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML;
            let newColumnNameEle = document.getElementById('columnNameText' + tableNumber + tableColumnNumber + tableColumnNumber);
            if (newColumnNameEle) {
              newColumnName = newColumnNameEle.value;
            }
            let originalColumnName = schemaConversionObj.ToSpanner[tableName].Cols[srcColumnName];
            updatedColsData.UpdateCols[originalColumnName] = {};
            updatedColsData.UpdateCols[originalColumnName]['Removed'] = false;
            if (newColumnName === originalColumnName) {
              updatedColsData.UpdateCols[originalColumnName]['Rename'] = '';
            }
            else {
              let columnsLength = Object.keys(schemaConversionObj.ToSpanner[tableName].Cols).length;
              columnNameExists = false;
              for (let k = 0; k < columnsLength; k++) {
                if (k != tableColumnNumber && newColumnName == document.getElementById('columnNameText' + tableNumber + k + k).value) {
                  changesSuccess = false;
                  jQuery('#editTableWarningModal').modal();
                  jQuery('#editTableWarningModal').find('#modal-content').html("Column : '" + newColumnName + "'" + ' already exists in table : ' + "'" + tableName + "'" + '. Please try with a different column name.');
                  updatedColsData.UpdateCols[originalColumnName]['Rename'] = '';
                  columnNameExists = true;
                  break
                }
              }
              if (!columnNameExists)
                updatedColsData.UpdateCols[originalColumnName]['Rename'] = newColumnName;
            }
            updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'ADDED';
            updatedColsData.UpdateCols[originalColumnName]['PK'] = '';
            updatedColsData.UpdateCols[originalColumnName]['ToType'] = document.getElementById('dataType' + tableNumber + tableColumnNumber + tableColumnNumber).value;

            if (notNullConstraint[parseInt(String(tableNumber) + String(tableColumnNumber))] === 'Not Null') {
              updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'ADDED';
            }
            else if (notNullConstraint[parseInt(String(tableNumber) + String(tableColumnNumber))] === '') {
              updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'REMOVED';
            }
            if (!(jQuery(this).find("input[type=checkbox]").is(":checked"))) {
              updatedColsData.UpdateCols[originalColumnName]['Removed'] = true;
            }
            tableColumnNumber++;
          }
        });
        switch (columnNameExists) {
          case true:
            // store previous state
            break
          case false:
            tableData = await Fetch.getAppData('POST', '/typemap/table?table=' + tableName, updatedColsData);
            if (tableData.ok) {
              changesSuccess = true;
              tableData = await tableData.text();
              localStorage.setItem('conversionReportContent', tableData);
            }
            else {
              changesSuccess = false;
              tableData = await tableData.text();
              jQuery('#editTableWarningModal').modal();
              jQuery('#editTableWarningModal').find('#modal-content').html(tableData);
            }
        }
        // save fk handler
        if (schemaConversionObj.SpSchema[tableName].Fks != null && schemaConversionObj.SpSchema[tableName].Fks.length != 0) {
          fkLength = schemaConversionObj.SpSchema[tableName].Fks.length;
          for (let x = 0; x < fkLength; x++) {
            let newFkVal = document.getElementById('newFkVal' + tableNumber + x).value;
            if (schemaConversionObj.SpSchema[tableName].Fks[x].Name != newFkVal)
              renameFkMap[schemaConversionObj.SpSchema[tableName].Fks[x].Name] = newFkVal;
          }
          if (Object.keys(renameFkMap).length > 0) {
            let duplicateCheck = [];
            let duplicateFound = false;
            let keys = Object.keys(renameFkMap);
            keys.forEach(function (key) {
              for (let x = 0; x < fkLength; x++) {
                if (schemaConversionObj.SpSchema[tableName].Fks[x].Name === renameFkMap[key]) {
                  changesSuccess = false;
                  jQuery('#editTableWarningModal').modal();
                  jQuery('#editTableWarningModal').find('#modal-content').html("Foreign Key: " + renameFkMap[key] + " already exists in table: " + tableName + ". Please try with a different name.");
                  duplicateFound = true;
                }
              }
              if (duplicateCheck.includes(renameFkMap[key])) {
                changesSuccess = false;
                jQuery('#editTableWarningModal').modal();
                jQuery('#editTableWarningModal').find('#modal-content').html('Please use a different name for each foreign key');
                duplicateFound = true;
              }
              else {
                duplicateCheck.push(renameFkMap[key]);
              }
            });
            switch (duplicateFound) {
              case true:
                // store previous state
                break;
              case false:
                fkTableData = await Fetch.getAppData('POST', '/rename/fks?table=' + tableName, renameFkMap);
                if (!fkTableData.ok) {
                  changesSuccess = false;
                  fkTableData = await fkTableData.text();
                  jQuery('#editTableWarningModal').modal();
                  jQuery('#editTableWarningModal').find('#modal-content').html(fkTableData);
                }
                else {
                  changesSuccess = true;
                  fkTableData = await fkTableData.text();
                  localStorage.setItem('conversionReportContent', fkTableData);
                }
                break;
            }
          }
        }
        // save Secondary Index handler
        if (schemaConversionObj.SpSchema[tableName].Indexes != null && schemaConversionObj.SpSchema[tableName].Indexes.length != 0) {
          secIndexLength = schemaConversionObj.SpSchema[tableName].Indexes.length;
          for (let x = 0; x < secIndexLength; x++) {
            let newSecIndexVal = document.getElementById('newSecIndexVal' + tableNumber + x).value;
            if (schemaConversionObj.SpSchema[tableName].Indexes[x].Name != newSecIndexVal)
              renameIndexMap[schemaConversionObj.SpSchema[tableName].Indexes[x].Name] = newSecIndexVal;
          }
          if (Object.keys(renameIndexMap).length > 0) {
            let duplicateCheck = [];
            let duplicateFound = false;
            let keys = Object.keys(renameIndexMap);
            keys.forEach(function (key) {
              for (let x = 0; x < secIndexLength; x++) {
                if (schemaConversionObj.SpSchema[tableName].Indexes[x].Name === renameIndexMap[key]) {
                  changesSuccess = false;
                  jQuery('#editTableWarningModal').modal();
                  jQuery('#editTableWarningModal').find('#modal-content').html("Index: " + renameIndexMap[key] + " already exists in table: " + tableName + ". Please try with a different name.");
                  duplicateFound = true;
                }
              }
              if (duplicateCheck.includes(renameIndexMap[key])) {
                changesSuccess = false;
                jQuery('#editTableWarningModal').modal();
                jQuery('#editTableWarningModal').find('#modal-content').html('Please use a different name for each secondary index');
                duplicateFound = true;
              }
              else {
                duplicateCheck.push(renameIndexMap[key]);
              }
            });
            switch (duplicateFound) {
              case true:
                // store previous state
                break;
              case false:
                secIndexTableData = await Fetch.getAppData('POST', '/rename/indexes?table=' + tableName, renameIndexMap);
                if (!secIndexTableData.ok) {
                  changesSuccess = false;
                  secIndexTableData = await secIndexTableData.text();
                  jQuery('#editTableWarningModal').modal();
                  jQuery('#editTableWarningModal').find('#modal-content').html(secIndexTableData);
                }
                else {
                  changesSuccess = true;
                  secIndexTableData = await secIndexTableData.text();
                  localStorage.setItem('conversionReportContent', secIndexTableData);
                }
                break;
            }
          }
        }
        if (changesSuccess) {
          let updatedData = JSON.parse(localStorage.getItem('conversionReportContent'));
          event.target.innerHTML = "Edit Spanner Schema";
          document.getElementById("editInstruction" + tableNumber).style.visibility = "visible";
          jQuery(tableId).each(function () {
            jQuery(this).find('.src-tab-cell .bmd-form-group').remove();
          });
          if (updatedData.SpSchema[tableName].Fks != null && updatedData.SpSchema[tableName].Fks.length != 0) {
            fkLength = updatedData.SpSchema[tableName].Fks.length;
            for (let x = 0; x < fkLength; x++) {
              jQuery('#renameFk' + tableNumber + x).addClass('template');
            }
          }
          if (updatedData.SpSchema[tableName].Indexes != null && updatedData.SpSchema[tableName].Indexes.length != 0) {
            secIndexLength = updatedData.SpSchema[tableName].Indexes.length;
            for (let x = 0; x < secIndexLength; x++) {
              jQuery('#renameSecIndex' + tableNumber + x).addClass('template');
            }
          }
          Store.updateSchemaScreen(localStorage.getItem('conversionReportContent'));
        }
      }
    },
    dropForeignKeyHandler: async (tableName, tableNumber, pos) => {
      let response;
      response = await Fetch.getAppData('GET', '/drop/fk?table=' + tableName + '&pos=' + pos);
      if (response.ok) {
        let responseCopy = response.clone();
        let jsonResponse = await responseCopy.json();
        let textRresponse = await response.text();
        localStorage.setItem('conversionReportContent', textRresponse);
        if (jsonResponse.SpSchema[tableName].Fks != null && jsonResponse.SpSchema[tableName].Fks.length != 0) {
          let table = document.getElementById('fkTableBody' + tableNumber);
          let rowCount = table.rows.length;
          let keyFound;
          let z;
          for (let x = 0; x < rowCount; x++) {
            keyFound = false;
            for (let y = 0; y < jsonResponse.SpSchema[tableName].Fks.length; y++) {
              let oldFkVal = jQuery('#saveFk' + tableNumber + x).removeClass('template').html();
              if (jsonResponse.SpSchema[tableName].Fks[y].Name === oldFkVal) {
                jQuery('#saveFk' + tableNumber + x).addClass('template');
                document.getElementById(tableName + x + 'foreignKey').id = tableName + y + 'foreignKey';
                document.getElementById('saveFk' + tableNumber + x).id = 'saveFk' + tableNumber + y;
                document.getElementById('renameFk' + tableNumber + x).id = 'renameFk' + tableNumber + y;
                document.getElementById('newFkVal' + tableNumber + x).id = 'newFkVal' + tableNumber + y;
                keyFound = true;
                break;
              }
            }
            if (keyFound == false) {
              z = x;
            }
          }
          table.deleteRow(z);
        }
        else {
          jQuery('#' + tableNumber).find('.fkCard').addClass('template');
        }
      }
    },
    dropSecondaryIndexHandler: async (tableName, tableNumber, pos) => {
      let response;
      response = await Fetch.getAppData('GET', '/drop/secondaryindex?table=' + tableName + '&pos=' + pos);
      if (response.ok) {
        let responseCopy = response.clone();
        let jsonObj = await responseCopy.json();
        let textRresponse = await response.text();
        localStorage.setItem('conversionReportContent', textRresponse);
        let secIndexArray = jsonObj.SpSchema[tableName].Indexes;
        let tablemap = document.querySelector("#indexKey" + tableNumber).querySelector(".index-acc-table.fkTable");
        let flag = document.querySelector("#editSpanner" + tableNumber).innerHTML === "Save Changes";

        tablemap.innerHTML = ` ${secIndexArray && secIndexArray.length > 0 ? 
        `<thead>
          <tr>
              <th>Name</th>
              <th>Table</th>
              <th>Unique</th>
              <th>Keys</th>
              <th>Action</th>
          </tr>
        </thead>`
        :
        `<div></div>`} 
        ${secIndexArray.map((secIndex, index) => {
          return `
            <tr class="indexTableTr ">
                <td class="acc-table-td indexesName">
                    <div class="renameSecIndex ${flag ? '' : 'template'}" id="renameSecIndex${tableNumber}${index}">
                        <input type="text" id="newSecIndexVal${tableNumber}${index}" value="${secIndex.Name}"
                            class="form-control spanner-input" autocomplete="off" />
                    </div>
                    <div class="saveSecIndex ${flag ? 'template' : ''}" id="saveSecIndex${tableNumber}${index}">${secIndex.Name}</div>
                    
                </td>
                <td class="acc-table-td indexesTable">${secIndex.Table}</td>
                <td class="acc-table-td indexesUnique">${secIndex.Unique}</td>
                <td class="acc-table-td indexesKeys">${secIndex.Keys.map((key) => key.Col).join(',')}</td>
                <td class="acc-table-td indexesAction">
                    <button class="dropButton" id="${tableName}${index}secIndex" ${flag ? "" : "disabled"}>
                        <span><i class="large material-icons removeIcon"
                                style="vertical-align: middle">delete</i></span>
                        <span style="vertical-align: middle">Drop</span>
                    </button>
                </td>
            </tr>
          `;
        }).join("")}`;
        if (secIndexArray !== null && secIndexArray.length > 0) {
          secIndexArray.map((secIndex, index) => {
            document.getElementById(tableName + index + 'secIndex').addEventListener('click', () => {
              jQuery('#indexAndKeyDeleteWarning').modal();
              jQuery('#indexAndKeyDeleteWarning').find('#modal-content').html(`This will permanently delete the secondary index and the corresponding uniqueness constraints on
                indexed columns (if applicable). Do you want to continue?`);
              recreateNode(document.getElementById('fk-drop-confirm'))
              document.getElementById('fk-drop-confirm').addEventListener('click', () => {
                Actions.dropSecondaryIndexHandler(tableName, tableNumber, index);
              })
            })
          });
        }
      }
    },
    showSpinner : () => {
      let toggle_spinner = document.getElementById("toggle-spinner");
      toggle_spinner.style.display = "block";
    },
    hideSpinner : () => {
     let toggle_spinner = document.getElementById("toggle-spinner");
      toggle_spinner.style.display = "none";
      toggle_spinner.className = toggle_spinner.className.replace("show", "");
    }
    
  };
})();

export default Actions;
