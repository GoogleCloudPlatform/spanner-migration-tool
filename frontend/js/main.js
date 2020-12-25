/**
 * Function to initiate home screen tasks like validating form input fields
 *
 * @return {null}
 */
const initHomeScreenTasks = () => {
  jQuery(document).ready(function () {
    setActiveSelectedMenu('homeScreen');
    jQuery('#loadDbForm > div > input').keyup(function () {
      var empty = false;
      jQuery('#loadDbForm > div > input').each(function () {
        if (jQuery(this).val() === '') {
          empty = true;
        }
      });
      if (empty) {
        jQuery('#loadConnectButton').attr('disabled', 'disabled');
      } else {
        jQuery('#loadConnectButton').removeAttr('disabled');
      }
    });
    jQuery('#connectForm > div > input').keyup(function () {
      var empty = false;
      jQuery('#connectForm > div > input').each(function () {
        if (jQuery(this).val() === '') {
          empty = true;
        }
      });
      if (empty) {
        jQuery('#connectButton').attr('disabled', 'disabled');
      }
      else {
        jQuery('#connectButton').removeAttr('disabled');
      }
    });
  })
}

/**
 * Function to trigger click event while file uploading
 *
 * @return {null}
 */
const uploadFileHandler = (e) => {
  e.preventDefault();
  jQuery("#upload:hidden").trigger('click');
}

/**
 * Function to update selected file name and to read the json content of selected file while file uploading while file uploading
 *
 * @return {null}
 */
const filenameChangeHandler = () => {
  let fileName = jQuery('#upload')[0].files[0].name;
  if (fileName != '') {
    jQuery('#importButton').removeAttr('disabled');
  }
  jQuery("#upload_link").text(fileName);

  let reader = new FileReader();
  reader.onload = function (event) {
    let importSchemaObj = JSON.parse(event.target.result);
    localStorage.setItem('conversionReportContent', JSON.stringify(importSchemaObj));
    localStorage.setItem('importFileName', fileName);
    localStorage.setItem('importFilePath', 'frontend/');
  }
  reader.readAsText(event.target.files[0]);
}

/**
 * Function to create table from json structure
 *
 * @param {json} obj Json object contaning source and spanner table information
 * @return {null}
 */
const createSourceAndSpannerTables = async(obj) => {
  schemaConversionObj = obj;
  let spannerColumnsContent, columnNameContent, dataTypeContent, constraintsContent, notNullFound, constraintId, srcConstraintHtml;
  let pksSp = [], notPrimary = [], keyColumnMap = [], initialColNameArray = [], notNullFoundFlag = [], pkSeqId = [], pkArray = [], initialPkSeqId = [], constraintTabCell = [], primaryTabCell = [], spPlaceholder = [], srcPlaceholder = [], countSp = [], countSrc = [];
  let tableContent = '';
  let sourceTableFlag = '';
  let conversionRateResp = {};
  let constraintCount = 0;
  let accordion = document.getElementById("accordion");
  let srcTableNum = Object.keys(schemaConversionObj.SrcSchema).length;
  let spTable_num = Object.keys(schemaConversionObj.SpSchema).length;
  let reportUl = document.createElement('ul');
  reportUl.setAttribute('id', 'reportUl');
  getFilePaths();
  fetch('/typemap', {
    method: 'GET',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }
  })
  .then(function (res) {
    if (res.ok) {
      res.json().then(function (result) {
        globalDataTypes = result;
        localStorage.setItem('globalDataTypeList', JSON.stringify(globalDataTypes));
      });
    }
    else {
      return Promise.reject(res);
    }
  }).catch(function (err) {
    showSnackbar(err, ' redBg');
  });

  for (var x = 0; x < srcTableNum; x++) {
    initialPkSeqId[x] = [];
    initialColNameArray[x] = [];
    constraintTabCell[x] = [];
    primaryTabCell[x] = [];
    notPrimary[x] = [];
    notNullFoundFlag[x] = [];
    keyColumnMap[x] = [];
    pkArray[x] = [];
    spPlaceholder[x] = [];
    countSp[x] = [];
    countSrc[x] = [];
    pksSp[x] = [];
  }

  conversionRateResp = JSON.parse(localStorage.getItem('tableBorderColor'));
  for (var i = 0; i < srcTableNum; i++) {
    let srcTable = schemaConversionObj.SrcSchema[Object.keys(schemaConversionObj.ToSpanner)[i]];
    srcTableName[i] = Object.keys(schemaConversionObj.ToSpanner)[i];
    let spTable = schemaConversionObj.SpSchema[srcTableName[i]];
    let spTableCols = spTable.ColNames;
    pkArray[i] = schemaConversionObj.SpSchema[Object.keys(schemaConversionObj.SpSchema)[i]].Pks;
    pkSeqId[i] = 1;
    let pkArrayLength = pkArray[i].length;
    let columnsLength = Object.keys(schemaConversionObj.ToSpanner[spTable.Name].Cols).length;
    for (var x = 0; x < pkArrayLength; x++) {
      pkArray[i][x].seqId = pkSeqId[i];
      pkSeqId[i]++;
    }
    schemaConversionObj.SpSchema[srcTableName[i]].Pks = pkArray[i];
    spannerColumnsContent = '';
    for (var k = 0; k < columnsLength; k++) {
      spannerColumnsContent += `<tr>`
      let currentColumnSrc = Object.keys(schemaConversionObj.ToSpanner[spTable.Name].Cols)[k];
      let currentColumnSp = schemaConversionObj.ToSpanner[spTable.Name].Cols[currentColumnSrc];
      for (var l = 0; l < 2; l++) {
        columnNameContent = '';
        if (l % 2 === 0) {
          columnNameContent += `<td class='acc-table-td src-tab-cell'>`
          if (srcTable.PrimaryKeys !== null && srcTable.PrimaryKeys[0].Column === currentColumnSrc) {
            columnNameContent += `<span class="column left">
                                  <img src='./Icons/Icons/ic_vpn_key_24px.svg' style='margin-left: 3px;'>
                                </span>
                                <span class="column right srcColumn" id='srcColumn${k}'>
                                  ${currentColumnSrc}
                                </span>`;
          }
          else {
            columnNameContent += `<span class="column left">
                                  <img src='./Icons/Icons/ic_vpn_key_24px-inactive.svg' style='visibility: hidden; margin-left: 3px;'>
                                </span>
                                <span class="column right srcColumn" id='srcColumn${k}'>
                                  ${currentColumnSrc}
                                </span>`
          }
          columnNameContent += `</td>`;
        }
        else {
          columnNameContent += `<td class='sp-column acc-table-td spannerTabCell${i}${k}'>`
          pksSp[i] = [...spTable.Pks];
          pkFlag = false;
          let pksSpLength = pksSp[i].length;
          for (var x = 0; x < pksSpLength; x++) {
            if (pksSp[i][x].Col === currentColumnSp) {
              pkFlag = true;
              columnNameContent += `<span class="column left" data-toggle="tooltip" data-placement="bottom" title="primary key : ${spTableCols[k]}" id='keyIcon${i}${k}${k}' style="cursor:pointer">
                                    <sub>${pksSp[i][x].seqId}</sub><img src='./Icons/Icons/ic_vpn_key_24px.svg' class='primaryKey'>
                                  </span>
                                  <span class="column right" data-toggle="tooltip" data-placement="bottom" title="primary key : ${spTableCols[k]}" id='columnNameText${i}${k}${k}' style="cursor:pointer">
                                    ${currentColumnSp}
                                  </span>`;
              notPrimary[i][k] = false;
              initialPkSeqId[i][k] = pksSp[i][x].seqId;
              break
            }
          }
          if (pkFlag === false) {
            notPrimary[i][k] = true;
            columnNameContent += `<span class="column left" id='keyIcon${i}${k}${k}'>
                                  <img src='./Icons/Icons/ic_vpn_key_24px-inactive.svg' style='visibility: hidden;'>
                                </span>
                                <span class="column right" id='columnNameText${i}${k}${k}'>
                                  ${currentColumnSp}
                                </span>`;
          }
          columnNameContent += `</td>`;
          primaryTabCell[i][k] = columnNameContent;
          keyIconValue = 'keyIcon' + i + k + k;
          keyColumnObj = { 'keyIconId': keyIconValue, 'columnName': currentColumnSp };
          keyColumnMap[i].push(keyColumnObj);
        }
        spannerColumnsContent += columnNameContent;
      }

      for (var l = 0; l < 2; l++) {
        dataTypeContent = '';
        notNullFound = ''
        if (l % 2 === 0) {
          dataTypeContent += `<td class='acc-table-td pl-data-type' id='srcDataType${i}${k}'>${srcTable.ColDefs[currentColumnSrc].Type.Name}</td>`;
        }
        else {
          dataTypeContent += `<td class='sp-column acc-table-td spannerTabCell${i}${k}' id='dataType${i}${k}'>${spTable.ColDefs[currentColumnSp].T.Name}</td>`;
        }
        spannerColumnsContent += dataTypeContent;
      }

      for (var l = 0; l < 2; l++) {
        constraintsContent = '';
        if (l % 2 === 0) {
          constraintsContent += `<td class='acc-table-td'>`
          countSrc[i][k] = 0;
          srcPlaceholder[constraintCount] = countSrc[i][k];
          if (srcTable.ColDefs[currentColumnSrc].NotNull !== undefined) {
            if (srcTable.ColDefs[currentColumnSrc].NotNull === true) {
              countSrc[i][k] = countSrc[i][k] + 1;
              srcPlaceholder[constraintCount] = countSrc[i][k];
              notNullFound = "<option disabled class='active'>Not Null</option>";
            }
            else {
              notNullFound = "<option disabled>Not Null</option>";
            }
          }
          else {
            notNullFound = '';
          }
    
          constraintId = 'srcConstraint' + constraintCount;
          srcConstraintHtml = "<select id=" + constraintId + " multiple size='1' class='form-control spanner-input tableSelect'>"
            + notNullFound
            + "</select>";
          constraintsContent += srcConstraintHtml;
          constraintsContent += `</td>`;
          constraintCount++;
        }
        else {
          constraintsContent += `<td class='acc-table-td sp-column acc-table-td spannerTabCell${i}${k}'>`;
          countSp[i][k] = 0;
          spPlaceholder[i][k] = countSp[i][k];
          // checking not null consraint
          if (spTable.ColDefs[currentColumnSp].NotNull !== undefined) {
            if (spTable.ColDefs[currentColumnSp].NotNull === true) {
              countSp[i][k] = countSp[i][k] + 1
              spPlaceholder[i][k] = countSp[i][k];
              notNullFound = "<option disabled class='active'>Not Null</option>";
              notNullFoundFlag[i][k] = true;
              notNullConstraint[parseInt(String(i) + String(k))] = 'Not Null';
            }
            else {
              notNullFound = "<option disabled>Not Null</option>";
              notNullFoundFlag[i][k] = false;
              notNullConstraint[parseInt(String(i) + String(k))] = '';
            }
          }
          else {
            notNullFound = "<option disabled>Not Null</option>";
            notNullFoundFlag[i][k] = false;
          }
          constraintId = 'spConstraint' + i + k;
          spConstraintHtml = "<select id=" + constraintId + " multiple size='1' class='form-control spanner-input tableSelect'>"
            + notNullFound
            + "</select>";
          constraintsContent += spConstraintHtml;
          constraintsContent += `</td>`;
          constraintTabCell[i][k] = constraintsContent;
        }
        spannerColumnsContent += constraintsContent;
      }
      spannerColumnsContent += `</tr>`;
    }
    sourceTableFlag = localStorage.getItem('sourceDbName');
    tableContent =  `<section>
                        <div class='card' id=${i}>
                          <div role='tab' class='card-header report-card-header borderBottom ${panelBorderClass(conversionRateResp[srcTableName[i]])}'>
                            <h5 class='mb-0'>
                              <a href='#${Object.keys(schemaConversionObj.SrcSchema)[i]}' data-toggle='collapse'>
                                Table: ${Object.keys(schemaConversionObj.SrcSchema)[i]} <i class="fas fa-angle-down rotate-icon"></i>
                              </a>
                              <span class='spanner-text right-align hide-content'>Spanner</span>
                              <span class='spanner-icon right-align hide-content'>
                                <i class='large material-icons' style='font-size: 18px;'>circle</i>
                              </span>
                              <span class='source-text right-align hide-content'>Source</span>
                              <span class='source-icon right-align hide-content'>
                                <i class='large material-icons' style='font-size: 18px;'></i>
                              </span>
                              <button class='right-align edit-button hide-content' id='editSpanner${i}' onclick='editAndSaveButtonHandler(jQuery(this), ${JSON.stringify(spPlaceholder)}, ${JSON.stringify(pkArray[i])}, ${pkSeqId[i]}, ${JSON.stringify(notNullFoundFlag[i])}, ${JSON.stringify(initialColNameArray[i])}, ${JSON.stringify(keyColumnMap[i])}, ${JSON.stringify(notPrimary[i])}, ${JSON.stringify(pksSp[i])})'>
                                Edit Spanner Schema
                              </button>
                            </h5>
                          </div>

                          <div class='collapse reportCollapse' id='${Object.keys(schemaConversionObj.SrcSchema)[i]}'>
                            <div class='mdc-card mdc-card-content table-card-border ${mdcCardBorder(conversionRateResp[srcTableName[i]])}'>
                              <div class='acc-card-content' id='acc_card_content'>
                                <table id='src-sp-table${i}' class='acc-table'>
                                  <thead>
                                    <tr>
                                      <th class='acc-column' colspan='2'>Column Name</th>
                                      <th class='acc-column' colspan='2'>Data Type</th>
                                      <th class='acc-column' colspan='2'>Constraints</th>
                                    </tr>
                                    <tr>
                                      <th class='acc-table-th-src src-tab-cell'>${sourceTableFlag}</th>
                                      <th class='acc-table-th-spn'>Spanner</th>
                                      <th class='acc-table-th-src'>${sourceTableFlag}</th>
                                      <th class='acc-table-th-spn'>Spanner</th>
                                      <th class='acc-table-th-src'>${sourceTableFlag}</th>
                                      <th class='acc-table-th-spn'>Spanner</th>
                                    </tr>
                                  </thead>

                                  <tbody>
                                    ${spannerColumnsContent}
                                  </tbody>
                                </table>` + 
                                  foreignKeyHandler(i, spTable.Fks)
                                 +
                                 createSummaryForEachTable(i, JSON.parse(localStorage.getItem('summaryReportContent')))
                                 +
                              `</div>
                            </div>
                          </div>
                        </div>
                      </section>`;
    reportUl.innerHTML += tableContent;
  }
  if (accordion) {
    accordion.appendChild(reportUl);
  }
  constraintCount--;
  while (constraintCount >= 0) {
    if (document.getElementById('srcConstraint' + constraintCount) != null) {
      new vanillaSelectBox('#srcConstraint' + constraintCount, {
        placeHolder: srcPlaceholder[constraintCount] + " constraints selected",
        maxWidth: 500,
        maxHeight: 300
      });
    }
    constraintCount--;
  }

  for (var i = 0; i < srcTableNum; i++) {
    let tableId = '#src-sp-table' + i;
    jQuery(tableId).DataTable();
  }

  for (var i = 0; i < spTable_num; i++) {
    let spTable = schemaConversionObj.SpSchema[Object.keys(schemaConversionObj.SpSchema)[i]]
    let spTableCols = spTable.ColNames;
    let spTableColsLength = spTableCols.length;
    for (var j = 0; j < spTableColsLength; j++) {
      if (document.getElementById('spConstraint' + i + j) != null) {
        new vanillaSelectBox('#spConstraint' + i + j, {
          placeHolder: spPlaceholder[i][j] + " constraints selected",
          maxWidth: 500,
          maxHeight: 300
        });
      }
    }
  }
  tooltipHandler();
}

/**
 * Function to handle click event on edit spanner schema button of table
 *
 * @param {HTMLElement} event click event
 * @param {array} spPlaceholder array to store number of selected constraints in spanner constraint cell
 * @param {array} tablePkArray array to store primary keys of a table
 * @param {number} pkSeqId sequence number of primary key
 * @param {array} columnsNotNullConstraint array to store not null constraint value for all columns of a particular table
 * @param {array} tableOriginalColNames array to store original column names of a particular table
 * @param {array} keyColumnMap array to store primary key and column as a map of a particular table
 * @param {array} notPrimaryArray array to store whether column of a particular table is PK or not
 * @param {array} pkSpArray
 * @return {null}
 */
const editAndSaveButtonHandler = (event, spPlaceholder, tablePkArray, pkSeqId, columnsNotNullConstraint, tableOriginalColNames, keyColumnMap, notPrimaryArray, pkSpArray) => {
  if (event[0].innerText === "Edit Spanner Schema") {
    editSpannerHandler(event, tablePkArray, pkSeqId, columnsNotNullConstraint, tableOriginalColNames, keyColumnMap, notPrimaryArray);
  }
  else if (event[0].innerText === "Save Changes") {
    saveSpannerChanges(event, spPlaceholder, tableOriginalColNames, notPrimaryArray, pkSpArray);
  }
}

/**
 * Function to create foreign key tab for each table
 *
 * @param {number} index table index
 * @param {Array} foreignKeys foreign keys array for each table
 * @return {null}
 */
const foreignKeyHandler = (index, foreignKeys) => {
  if (foreignKeys == null) {
    return '';
  }
  let fkContentForTable = '';
  let fkTbodyContent = '';
  for (var p in foreignKeys) {
    fkTbodyContent = `<tr>`;
    for (var k in foreignKeys[p]) {
      fkTbodyContent += `<td class='acc-table-td'>${foreignKeys[p][k]}</td>`;
    }
    fkTbodyContent += `</tr>`
  }
  fkContentForTable = `<div class='summaryCard'>
                          <div class='foreignKeyHeader' role='tab'>
                            <h5 class='mb-0'>
                              <a class='summaryFont' data-toggle='collapse' href='#foreignKey${index}'>
                                Foreign Keys
                              </a>
                            </h5>
                          </div>

                          <div id='foreignKey${index}' class='collapse summaryCollapse'>
                            <div class='mdc-card mdc-card-content summaryBorder' style='border: 0px;'>
                              <div class='mdc-card summary-content'>
                                <fieldset style='overflow: hidden;'>
                                  <div class="radio-class">
                                    <input type="radio" class="radio" name="fks" value="add" id="add${index}" checked='checked' />
                                    <label style='margin-right: 15px;' for="add">Use as Foreign Key</label>
                                    <input type="radio" class="radio" name="fks" value="interleave" id="interleave${index}" />
                                    <label style='margin-right: 15px;' for="interleave">Convert to Interleave</label>
                                  </div>
                                  <button style='float: right; padding: 0px 20px;' class='edit-button' id='saveInterleave${index}' onclick='saveInterleaveHandler(${index})'>save</button>
                                </fieldset><br>

                                <table class='acc-table fkTable'>
                                  <thead>
                                    <tr>
                                      <th>Name</th>
                                      <th>Columns</th>
                                      <th>Refer Table</th>
                                      <th>Refer Columns</th>
                                    </tr>
                                  </thead>

                                  <tbody>
                                    ${fkTbodyContent}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          </div>
                       </div>`;
    return fkContentForTable;         
}

/**
 * Function to select foreign key behaviour in each table (convert to interleave or use as is)
 *
 * @param {number} index table index
 * @return {null}
 */
const saveInterleaveHandler = (index) => {
  const radioValues = document.querySelectorAll('input[name="fks"]');
  let selectedValue;
  for (const x of radioValues) {
      if (x.checked) {
          selectedValue = x.value;
          break;
      }
  }
  if (selectedValue == 'interleave') {
    console.log(index);
    console.log(interleaveApiCallResp[index]);
    if (interleaveApiCallResp[index].Possible == false) {
      showSnackbar('Cannot be Interleaved', ' redBg');  
    }
    else if (interleaveApiCallResp[index].Possible == true) {
      showSnackbar('Successfully Interleaved', ' greenBg');
    }
  }
  else {
    showSnackbar('Response Saved', ' greenBg');
  }
}

/**
 * Function to handle spanner table editing
 *
 * @param {event} event event generated by clicking edit spanner button
 * @param {array} tablePkArray array to store primary keys of a table
 * @param {number} pkSeqId sequence number of primary key
 * @param {array} columnsNotNullConstraint array to store not null constraint value for all columns of a particular table
 * @param {array} tableOriginalColNames array to store original column names of a particular table
 * @param {array} keyColumnMap array to store primary key and column as a map of a particular table
 * @param {array} notPrimaryArray array to store whether column of a particular table is PK or not
 * @return {null}
 */
const editSpannerHandler = (event, tablePkArray, pkSeqId, columnsNotNullConstraint, tableOriginalColNames, keyColumnMap, notPrimaryArray) => {
  let uncheckCount = [];
  if (event.html() === 'Edit Spanner Schema') {
    jQuery(event[0]).removeAttr('data-toggle');
  }
  event.html("Save Changes");

  let tableNumber = parseInt(event.attr('id').match(/\d+/), 10);
  let tableId = '#src-sp-table' + tableNumber + ' tr';
  let tableColumnNumber = 0;
  let tableCheckboxGroup = '.chckClass_' + tableNumber;
  uncheckCount[tableNumber] = 0;

  jQuery(tableId).each(function (index) {
    if (index === 1) {
      var temp = jQuery(this).find('.src-tab-cell');
      temp.prepend(`<span class="bmd-form-group is-filled">
                      <div class="checkbox">
                        <label>
                          <input type="checkbox" id='chckAll_${tableNumber}' value="">
                          <span class="checkbox-decorator"><span class="check" style='margin-left: -7px;'></span><div class="ripple-container"></div></span>
                        </label>
                      </div>
                    </span>`)
    }
    var checkAllTableNumber = jQuery('#chckAll_' + tableNumber);
    var checkClassTableNumber = jQuery('.chckClass_' + tableNumber);
    checkAllTableNumber.prop('checked', true);
    checkAllTableNumber.click(function () {
      tableNumber = parseInt(jQuery(this).attr('id').match(/\d+/), 10);
      checkClassTableNumber = jQuery('.chckClass_' + tableNumber);
      switch (jQuery(this).is(':checked')) {
        case true:
          checkClassTableNumber.prop('checked', true);
          break;
        case false:
          checkClassTableNumber.prop('checked', false);
          break;
      }
    });

    if (index > 1) {
      var temp = jQuery(this).find('.src-tab-cell');
      temp.prepend(`<span class="bmd-form-group is-filled">
                      <div class="checkbox">
                        <label>
                          <input type="checkbox" id="chckBox_${tableColumnNumber}" value="" class="chckClass_${tableNumber}">
                          <span class="checkbox-decorator"><span class="check"></span><div class="ripple-container"></div></span>
                        </label>
                      </div>
                    </span>`)
      jQuery(tableCheckboxGroup).prop('checked', true);
      let spannerCellsList = document.getElementsByClassName('spannerTabCell' + tableNumber + tableColumnNumber);
      if (spannerCellsList) {
        editSpannerColumnName(spannerCellsList[0], tableNumber, tableColumnNumber, tableId, tablePkArray, pkSeqId, tableOriginalColNames, keyColumnMap, notPrimaryArray);
        editSpannerDataType(spannerCellsList[1], tableNumber, tableColumnNumber);
        editSpannerConstraint(spannerCellsList[2], tableNumber, tableColumnNumber, columnsNotNullConstraint);
      }
      tableColumnNumber++;
    }
  });
  checkClassTableNumber = jQuery('.chckClass_' + tableNumber);
  checkClassTableNumber.click(function () {
    tableNumber = parseInt(jQuery(this).closest("table").attr('id').match(/\d+/), 10);
    tableColumnNumber = parseInt(jQuery(this).attr('id').match(/\d+/), 10);
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
}

/**
 * Function to edit column name for spanner table
 *
 * @param {html Element} editColumn
 * @param {number} tableNumber
 * @param {number} tableColumnNumber
 * @param {string} tableId
 * @param {array} tablePkArray array to store primary keys of a table
 * @param {number} pkSeqId sequence number of primary key
 * @param {array} tableOriginalColNames array to store original column names of a particular table
 * @param {array} keyColumnMap array to store primary key and column as a map of a particular table
 * @param {array} notPrimaryArray array to store whether column of a particular table is PK or not
 * @return {null}
 */
const editSpannerColumnName = (editColumn, tableNumber, tableColumnNumber, tableId, tablePkArray, pkSeqId, tableOriginalColNames, keyColumnMap, notPrimaryArray) => {
  let columnNameVal = document.getElementById('columnNameText' + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML;
  let currSeqId = '';
  let pkArrayLength = tablePkArray.length;
  tableOriginalColNames.push(columnNameVal);
  for (var x = 0; x < pkArrayLength; x++) {
    if (tablePkArray[x].Col === columnNameVal.trim()) {
      currSeqId = tablePkArray[x].seqId;
    }
  }
  if (notPrimaryArray[tableColumnNumber] === true) {
    editColumn.innerHTML = `<span class="column left keyNotActive keyMargin keyClick" id='keyIcon${tableNumber}${tableColumnNumber}${tableColumnNumber}'>
                              <img src='./Icons/Icons/ic_vpn_key_24px-inactive.svg'>
                            </span>
                            <span class="column right form-group">
                              <input id='columnNameText${tableNumber}${tableColumnNumber}${tableColumnNumber}' type="text" value=${columnNameVal} class="form-control spanner-input" autocomplete='off'>
                            </span>`
  }
  else {
    editColumn.innerHTML = `<span class="column left keyActive keyMargin keyClick" id='keyIcon${tableNumber}${tableColumnNumber}${tableColumnNumber}'>
                              <sub>${currSeqId}</sub><img src='./Icons/Icons/ic_vpn_key_24px.svg'>
                            </span>
                            <span class="column right form-group">
                              <input id='columnNameText${tableNumber}${tableColumnNumber}${tableColumnNumber}' type="text" value=${columnNameVal} class="form-control spanner-input" autocomplete='off'>
                            </span>`
  }
  jQuery('#keyIcon' + tableNumber + tableColumnNumber + tableColumnNumber).click(function () {
    jQuery(this).toggleClass('keyActive keyNotActive');
    let keyId = jQuery(this).attr('id');
    let keyColumnMapLength = keyColumnMap.length;
    for (var z = 0; z < keyColumnMapLength; z++) {
      if (keyId === keyColumnMap[z].keyIconId) {
        columnName = keyColumnMap[z].columnName;
      }
    }
    if (document.getElementById(keyId).classList.contains('keyActive')) {
      getNewSeqNumForPrimaryKey(keyId, tableNumber, tablePkArray, pkSeqId);
    }
    else {
      removePrimaryKeyFromSeq(tableNumber, tableId, tablePkArray, tableOriginalColNames, notPrimaryArray);
    }
  });
}

/**
 * Function to get new seq number for primary key
 *
 * @param {html id} keyId
 * @param {number} tableNumber specifies table number in json object
 * @param {array} tablePkArray array to store primary keys of a table
 * @param {number} pkSeqId sequence number of primary key
 * @return {null}
 */
const getNewSeqNumForPrimaryKey = (keyId, tableNumber, tablePkArray, pkSeqId) => {
  let maxSeqId = 0;
  let keyIdEle = document.getElementById(keyId);
  let pkArrayLength = tablePkArray.length;
  let pkFoundFlag = false;
  for (var z = 0; z < pkArrayLength; z++) {
    if (tablePkArray[z].seqId > maxSeqId) {
      maxSeqId = tablePkArray[z].seqId;
    }
  }
  maxSeqId = maxSeqId + 1;
  pkSeqId = maxSeqId;
  for (var z = 0; z < pkArrayLength; z++) {
    if (columnName != tablePkArray[z].Col) {
      pkFoundFlag = false;
    }
    else {
      pkFoundFlag = true;
      break;
    }
  }
  if (pkFoundFlag === false) {
    tablePkArray.push({ 'Col': columnName, 'seqId': pkSeqId });
  }
  schemaConversionObj.SpSchema[srcTableName[tableNumber]].Pks = tablePkArray;
  if (keyIdEle) {
    keyIdEle.innerHTML = `<sub>${pkSeqId}</sub><img src='./Icons/Icons/ic_vpn_key_24px.svg'>`;
  }
}

/**
 * Function to remove primary key from existing sequence
 *
 * @param {number} tableNumber
 * @param {string} tableId
 * @param {array} tablePkArray array to store primary keys of a table
 * @param {array} tableOriginalColNames array to store original column names of a particular table
 * @param {array} notPrimaryArray array to store whether column of a particular table is PK or not
 * @return {null}
 */
const removePrimaryKeyFromSeq = (tableNumber, tableId, tablePkArray, tableOriginalColNames, notPrimaryArray) => {
  let pkArrayLength = tablePkArray.length;
  let tableColumnNumber = 0;
  for (var z = 0; z < pkArrayLength; z++) {
    if (columnName === tablePkArray[z].Col) {
      tablePkArray.splice(z, 1);
      break;
    }
  }
  pkArrayLength = tablePkArray.length;
  for (var x = z; x < pkArrayLength; x++) {
    tablePkArray[x].seqId = tablePkArray[x].seqId - 1;
  }
  schemaConversionObj.SpSchema[srcTableName[tableNumber]].Pks = tablePkArray;
  jQuery(tableId).each(function (index) {
    if (index > 1) {
      notPrimaryArray[tableColumnNumber] = true;
      let currSeqId = '';
      for (var x = 0; x < pkArrayLength; x++) {
        if (tablePkArray[x].Col === tableOriginalColNames[tableColumnNumber].trim()) {
          currSeqId = tablePkArray[x].seqId;
          notPrimaryArray[tableColumnNumber] = false;
        }
      }
      if (notPrimaryArray[tableColumnNumber] === true) {
        document.getElementById('keyIcon' + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML = `<img src='./Icons/Icons/ic_vpn_key_24px-inactive.svg'>`;
      }
      if (notPrimaryArray[tableColumnNumber] === false) {
        document.getElementById('keyIcon' + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML = `<sub>${currSeqId}</sub><img src='./Icons/Icons/ic_vpn_key_24px.svg'>`;
      }
      tableColumnNumber++;
    }
  });
}

/**
 * Function to edit data type for spanner table
 *
 * @param {html Element} editColumn
 * @param {number} tableNumber
 * @param {number} tableColumnNumber
 * @return {null}
 */
const editSpannerDataType = (editColumn, tableNumber, tableColumnNumber) => {
  let spannerCellValue = editColumn.innerHTML;
  let srcCellValue;
  let srcCellValueEle = document.getElementById('srcDataType' + tableNumber + tableColumnNumber);
  if (srcCellValueEle) {
    srcCellValue = srcCellValueEle.innerHTML;
  }
  let dataTypeArray = null;
  let dataType = '';
  let globalDataTypesLength = Object.keys(globalDataTypes).length;
  for (var a = 0; a < globalDataTypesLength; a++) {
    if (srcCellValue.toLowerCase() === (Object.keys(globalDataTypes)[a]).toLowerCase()) {
      dataTypeArray = globalDataTypes[Object.keys(globalDataTypes)[a]];
      break;
    }
  }
  dataType = `<div class="form-group">
              <select class="form-control spanner-input tableSelect" id='dataType${tableNumber}${tableColumnNumber}${tableColumnNumber}'>`

  if (dataTypeArray !== null) {
    let dataTypeArrayLength = dataTypeArray.length;
    for (var a = 0; a < dataTypeArrayLength; a++) {
      dataType += `<option value=${dataTypeArray[a].T}>${dataTypeArray[a].T}</option>`
    }
  }
  else {
    dataType += `<option value=${spannerCellValue}>${spannerCellValue}</option>`
  }
  dataType += `</select> </div>`;
  editColumn.innerHTML = dataType;
}

/**
 * Function to edit constraint for spanner table
 *
 * @param {html Element} editColumn
 * @param {number} tableNumber
 * @param {number} tableColumnNumber
 * @param {array} columnsNotNullConstraint array to store not null constraint value for all columns of a particular table
 * @return {null}
 */
const editSpannerConstraint = (editColumn, tableNumber, tableColumnNumber, columnsNotNullConstraint) => {
  let notNullFound = '';
  let constraintId = 'spConstraint' + tableNumber + tableColumnNumber;
  if (columnsNotNullConstraint[tableColumnNumber] === true) {
    notNullFound = "<option class='active' selected>Not Null</option>";
  }
  else if (columnsNotNullConstraint[tableColumnNumber] === false) {
    notNullFound = "<option>Not Null</option>";
  }
  else {
    notNullFound = '';
  }
  constraintHtml = "<select id=" + constraintId + " multiple size='0' class='form-control spanner-input tableSelect' >"
    + notNullFound
    + "</select>";
  editColumn.innerHTML = constraintHtml;
  editColumn.setAttribute('class', 'sp-column acc-table-td spannerTabCell' + tableNumber + tableColumnNumber);
  new vanillaSelectBox("#spConstraint" + tableNumber + tableColumnNumber, {
    placeHolder: "Select Constraints",
    maxWidth: 500,
    maxHeight: 300
  });
  jQuery('#spConstraint' + tableNumber + tableColumnNumber).on('change', function () {
    let idNum = parseInt(jQuery(this).attr('id').match(/\d+/g), 10);
    let constraints = document.getElementById(constraintId);
    constraintId = jQuery(this).attr('id');
    notNullConstraint[idNum] = '';
    if (constraints) {
      let constraintsLength = constraints.length;
      for (var c = 0; c < constraintsLength; c++) {
        if (constraints.options[c].selected) {
          notNullConstraint[idNum] = 'Not Null';
        }
      }
    }
  });
}

/**
 * Function to save changes of spanner table
 *
 * @param {event} event event generated by clicking edit spanner button
 * @param {array} spPlaceholder array to store number of selected constraints in spanner constraint cell
 * @param {array} tableOriginalColNames array to store original column names of a particular table
 * @param {array} notPrimaryArray array to store whether column of a particular table is PK or not
 * @param {array} pkSpArray
 * @return {null}
 */
const saveSpannerChanges = (event, spPlaceholder, tableOriginalColNames, notPrimaryArray, pkSpArray) => {
  if (event.html() === 'Save Changes') {
    showSnackbar('changes saved successfully !!', ' greenBg');
  }
  event.html("Edit Spanner Schema");

  let tableNumber = parseInt(event.attr('id').match(/\d+/), 10);
  let tableId = '#src-sp-table' + tableNumber + ' tr';
  tableOriginalColNames = [];
  updatedColsData = {
    'UpdateCols': {
    }
  }
  jQuery(tableId).each(function (index) {
    if (index > 1) {
      tableName = srcTableName[tableNumber];
      let newColumnName;
      let tableColumnNumber = parseInt(jQuery(this).find('.srcColumn').attr('id').match(/\d+/), 10);
      let srcColumnName = jQuery(this).find('.srcColumn').html().trim()
      let spannerCellsList = document.getElementsByClassName('spannerTabCell' + tableNumber + tableColumnNumber);
      let newColumnNameEle = document.getElementById('columnNameText' + tableNumber + tableColumnNumber + tableColumnNumber);
      if (newColumnNameEle) {
        newColumnName = newColumnNameEle.value;
      }
      let originalColumnName = schemaConversionObj.ToSpanner[srcTableName[tableNumber]].Cols[srcColumnName];
      updatedColsData.UpdateCols[originalColumnName] = {};
      updatedColsData.UpdateCols[originalColumnName]['Removed'] = false;
      if (newColumnName === originalColumnName) {
        updatedColsData.UpdateCols[originalColumnName]['Rename'] = '';
      }
      else {
        updatedColsData.UpdateCols[originalColumnName]['Rename'] = newColumnName;
      }
      updatedColsData.UpdateCols[originalColumnName]['NotNull'] = '';
      updatedColsData.UpdateCols[originalColumnName]['PK'] = '';
      saveSpannerColumnName(spannerCellsList[0], tableNumber, tableColumnNumber, originalColumnName, newColumnName, notPrimaryArray, pkSpArray);
      updatedColsData.UpdateCols[originalColumnName]['ToType'] = document.getElementById('dataType' + tableNumber + tableColumnNumber + tableColumnNumber).value;
      saveSpannerConstraints(tableNumber, tableColumnNumber, originalColumnName);
      if (!(jQuery(this).find("input[type=checkbox]").is(":checked"))) {
        updatedColsData.UpdateCols[originalColumnName]['Removed'] = true;
      }
      new vanillaSelectBox('#spConstraint' + tableNumber + tableColumnNumber, {
        placeHolder: spPlaceholder[tableNumber][tableColumnNumber] + " constraints selected",
        maxWidth: 500,
        maxHeight: 300
      });
    }
  })

  jQuery(tableId).each(function () {
    jQuery(this).find('.src-tab-cell .bmd-form-group').remove();
  });
  tooltipHandler();

  fetch('/typemap/table?table=' + tableName, {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(updatedColsData)
  })
  .then(function (res) {
    if (res.ok) {
      res.json().then(async function (response) {
        localStorage.setItem('conversionReportContent', JSON.stringify(response));
        await ddlSummaryAndConversionApiCall();
        await getInterleaveInfo();
        const { component = ErrorComponent } = findComponentByPath(location.hash.slice(1).toLowerCase() || '/', routes) || {};
        document.getElementById('app').innerHTML = component.render();
        showSchemaConversionReportContent();
      });
    }
    else {
      return Promise.reject(res);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  });
}

/**
 * Function to save column name for spanner table
 *
 * @param {HTMLElement} saveColumn html element for column name
 * @param {number} tableNumber table number
 * @param {number} tableColumnNumber table column number
 * @param {string} originalColumnName 
 * @param {string} newColumnName
 * @param {array} notPrimaryArray array to store whether column of a particular table is PK or not
 * @param {array} pkSpArray
 * 
 * @return {null}
 */
const saveSpannerColumnName = (saveColumn, tableNumber, tableColumnNumber, originalColumnName, newColumnName, notPrimaryArray, pkSpArray) => {
  let currentPks = schemaConversionObj.SpSchema[srcTableName[tableNumber]].Pks;
  let pksSpLength = pkSpArray.length;
  let currentPksLength = currentPks.length;
  let foundOriginally;
  let currSeqId = '';
  if (document.getElementById('keyIcon' + tableNumber + tableColumnNumber + tableColumnNumber).classList.contains('keyActive')) {
    // checking if this key is newly added or removed
    foundOriginally = false;
    for (var z = 0; z < pksSpLength; z++) {
      if (originalColumnName === pkSpArray[z].Col) {
        foundOriginally = true;
        break;
      }
    }
    if (foundOriginally === false) {
      updatedColsData.UpdateCols[originalColumnName]['PK'] = 'ADDED'; 
    }

    for (var z = 0; z < currentPksLength; z++) {
      if (currentPks[z].Col === newColumnName) {
        currSeqId = currentPks[z].seqId;
      }
    }
    saveColumn.innerHTML = `
                        <span class="column left" data-toggle="tooltip" data-placement="bottom" title="primary key : ${document.getElementById('columnNameText' + tableNumber + tableColumnNumber + tableColumnNumber).value}" style="cursor:pointer">
                          <sub>${currSeqId}</sub><img src='./Icons/Icons/ic_vpn_key_24px.svg'>
                        </span>
                        <span class="column right" data-toggle="tooltip" data-placement="bottom" title="primary key : ${document.getElementById('columnNameText' + tableNumber + tableColumnNumber + tableColumnNumber).value}" id='columnNameText${tableNumber}${tableColumnNumber}${tableColumnNumber}' style="cursor:pointer">
                          ${document.getElementById('columnNameText' + tableNumber + tableColumnNumber + tableColumnNumber).value}
                        </span>`;
    notPrimaryArray[tableColumnNumber] = false;
  }
  else {

    // checking if this key is newly added or removed
    foundOriginally = false;
    for (var z = 0; z < pksSpLength; z++) {
      if (originalColumnName === pkSpArray[z].Col) {
        foundOriginally = true;
        updatedColsData.UpdateCols[originalColumnName]['PK'] = 'REMOVED';
        break;
      }
    }
  }
  notPrimaryArray[tableColumnNumber] = true;
}

/**
 * Function to save constraints for spanner table
 *
 * @param {number} tableNumber table number
 * @param {number} tableColumnNumber table column number
 * @param {string} originalColumnName
 * 
 * @return {null}
 */
const saveSpannerConstraints = (tableNumber, tableColumnNumber, originalColumnName) => {
  let constraintIndex = String(tableNumber) + String(tableColumnNumber);
  let notNullFound = '';
  constraintIndex = parseInt(constraintIndex);

  if (notNullConstraint[constraintIndex] === 'Not Null') {
    notNullFound = "<option disabled class='active' selected>Not Null</option>";
    updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'ADDED';
  }
  else if (notNullConstraint[constraintIndex] === '') {
    notNullFound = "<option disabled>Not Null</option>";
    updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'REMOVED';
  }
  constraintId = 'spConstraint' + tableNumber + tableColumnNumber;
                  constraintHtml = "<select id=" + constraintId + " multiple size='0' class='form-control spanner-input tableSelect' >"
                    + notNullFound
                    + "</select>";
}

/**
 * Function to create summary tab for each table
 *
 * @param {number} index table index
 * @param {json} summary json object containing summary for each table
 * @return {null}
 */
const createSummaryForEachTable = (index, summary) => {
  let summaryContentForTable = '';
  summaryContentForTable = `<div class='summaryCard'>
                              <div class='summaryCardHeader' role='tab'>
                                <h5 class='mb-0'>
                                  <a href='#viewSummary${index}' data-toggle='collapse' class='summaryFont'>View Summary</a>
                                </h5>
                              </div>

                              <div id='viewSummary${index}' class='collapse summaryCollapse'>
                                <div class='mdc-card mdc-card-content summaryBorder' style='border: 0px;'>
                                  <div class='mdc-card summary-content'>
                                    ${summary[srcTableName[index]].split('\n').join('<br />')}
                                  </div>
                                </div>
                              </div>
                            </div>`;
  return summaryContentForTable;
}

/**
 * Function to create summary tab for each table
 *
 * @param {json} result json object contaning summary for each table
 * @return {null}
 */
const createSummaryFromJson = (result) => {
  let summary = result;
  let summaryLength = Object.keys(summary).length;
  let summaryAccordion = document.getElementById('summary-accordion');
  let summaryUl = document.createElement('ul');
  let summaryContent = '';
  let conversionRateResp = {};
  conversionRateResp = JSON.parse(localStorage.getItem('tableBorderColor'));
  for (var i = 0; i < summaryLength; i++) {
    summaryContent += `<section>
                          <div class='card'>
                            <div class='card-header ddl-card-header ddlBorderBottom ${panelBorderClass(conversionRateResp[srcTableName[i]])}' role='tab'>
                              <h5 class='mb-0'>
                                <a data-toggle='collapse' href='#${Object.keys(summary)[i]}-summary'>
                                  Table: ${Object.keys(summary)[i]} <i class="fas fa-angle-down rotate-icon"></i>
                                </a>
                              </h5>
                            </div>

                            <div id='${Object.keys(summary)[i]}-summary' class='collapse summaryCollapse'>
                              <div class='mdc-card mdc-card-content ddl-border table-card-border ${mdcCardBorder(conversionRateResp[srcTableName[i]])}'>
                                <div class='mdc-card summary-content'>
                                  ${summary[srcTableName[i]].split('\n').join('<br />')}
                                </div>
                              </div>
                            </div>
                          </div>
                       </section>`
    summaryUl.innerHTML = summaryContent;
  }
  if (summaryAccordion) {
    summaryAccordion.appendChild(summaryUl);
  }
}

/**
 * Function to create ddl panel for each table
 *
 * @param {json} result json object contaning ddl statements for each table
 * @return {null}
 */
const createDdlFromJson = (result) => {
  let ddl = result;
  let ddlLength = Object.keys(ddl).length;
  let ddlAccordion = document.getElementById('ddl-accordion');
  let ddlUl = document.createElement('ul');
  let ddlContent = '';
  let conversionRateResp = {};
  conversionRateResp = JSON.parse(localStorage.getItem('tableBorderColor'));
  for (var i = 0; i < ddlLength; i++) {
    let createIndex = (ddl[Object.keys(ddl)[i]]).search('CREATE TABLE');
    let createEndIndex = createIndex + 12;
    ddl[Object.keys(ddl)[i]] = ddl[Object.keys(ddl)[i]].substring(0, createIndex) + ddl[Object.keys(ddl)[i]].substring(createIndex, createEndIndex).fontcolor('#4285f4').bold() + ddl[Object.keys(ddl)[i]].substring(createEndIndex);
    ddlContent += `<section>
                    <div class='card'>
                      <div class='card-header ddl-card-header ddlBorderBottom ${panelBorderClass(conversionRateResp[srcTableName[i]])}' role='tab'>
                        <h5 class='mb-0'>
                          <a href='#${Object.keys(ddl)[i]}-ddl' data-toggle='collapse'>Table: ${Object.keys(ddl)[i]} <i class="fas fa-angle-down rotate-icon"></i></a>
                        </h5>
                      </div>

                      <div id='${Object.keys(ddl)[i]}-ddl' class='collapse ddlCollapse'>
                        <div class='mdc-card mdc-card-content ddl-border table-card-border ${mdcCardBorder(conversionRateResp[srcTableName[i]])}'>
                          <div class='mdc-card ddl-content'>
                            <pre><code>${ddl[srcTableName[i]].split('\n').join(`<span class='sql-c'></span>`)}</code></pre>
                          </div>
                        </div>
                      </div>
                    </div>
                  </section>`
    ddlUl.innerHTML = ddlContent;
  }
  if (ddlAccordion) {
    ddlAccordion.appendChild(ddlUl);
  }
}

/**
 * Function to render edit schema screen from connect to DB mode
 *
 * @param {event} windowEvent hashchange or load event
 * @return {null}
 */
const showSchemaAssessment = async(windowEvent) => {
  let reportDataResp, reportData, sourceTableFlag;
  showSpinner();
  reportData = await fetch('/convert/infoschema')
  .then(function (response) {
    if (response.ok) {
      return response;
    }
    else {
      return Promise.reject(response);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  });
  reportDataResp = await reportData.json();
  localStorage.setItem('conversionReportContent', JSON.stringify(reportDataResp));
  await ddlSummaryAndConversionApiCall();
  await getInterleaveInfo();
  sourceTableFlag = localStorage.getItem('sourceDbName');
  jQuery('#connectModalSuccess').modal("hide");
  const { component = ErrorComponent } = findComponentByPath('/schema-report-connect-to-db', routes) || {};
  if (document.getElementById('app')) {
    document.getElementById('app').innerHTML = component.render();
  }
  showSchemaConversionReportContent();
  if (windowEvent == 'hashchange') {
    sessionRetrieval(sourceTableFlag);
  }
  showSnackbar('schema converted successfully !!', ' greenBg');
}

/**
 * Function to make conversion api call
 *
 * @return {null}
 */
const getConversionRate = async() => {
  let conversionRateResp, conversionRate;
  conversionRate = await fetch('/conversion')
  .then(function (response) {
    if (response.ok) {
      return response;
    }
    else {
      return Promise.reject(response);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  });
  conversionRateResp = await conversionRate.json();
  localStorage.setItem('tableBorderColor', JSON.stringify(conversionRateResp));
}

/**
 * Function to make ddl, summary and conversion api calls
 *
 * @return {null}
 */
const ddlSummaryAndConversionApiCall = async() => {
  let conversionRateResp, ddlDataResp, summaryDataResp;
  fetch('/ddl')
  .then(async function (response) {
    if (response.ok) {
      ddlDataResp = await response.json();
      localStorage.setItem('ddlStatementsContent', JSON.stringify(ddlDataResp));

      fetch('/summary')
      .then(async function (response) {
        if (response.ok) {
          summaryDataResp = await response.json();
          localStorage.setItem('summaryReportContent', JSON.stringify(summaryDataResp));

          fetch('/conversion')
          .then(async function (response) {
            if (response.ok) {
              conversionRateResp = await response.json();
              localStorage.setItem('tableBorderColor', JSON.stringify(conversionRateResp));
            }
            else {
              return Promise.reject(response);
            }
          })
          .catch(function (err) {
            showSnackbar(err, ' redBg');
          });
        }
        else {
          return Promise.reject(response);
        }
      })
      .catch(function (err) {
        showSnackbar(err, ' redBg');
      });
      
    }
    else {
      return Promise.reject(response);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  });
  
}

/**
 * Function to call create tables function for edit schema screen
 *
 * @return {null}
 */
const showSchemaConversionReportContent = () => {
  createSourceAndSpannerTables(JSON.parse(localStorage.getItem('conversionReportContent')));
  createDdlFromJson(JSON.parse(localStorage.getItem('ddlStatementsContent')));
  createSummaryFromJson(JSON.parse(localStorage.getItem('summaryReportContent')));
}

/**
 * Function to make an api call to get download file paths
 *
 * @return {null}
 */
const getFilePaths = () => {
  let filePathsResp;
  fetch('/filepaths')
  .then(async function (response) {
    if (response.ok) {
      filePathsResp = await response.json();
      localStorage.setItem('downloadFilePaths', JSON.stringify(filePathsResp));
    }
    else {
      filePaths = Promise.reject(response);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  });
}

/**
 * Function to store each session by making an api call
 *
 * @param {string} dbType source db name
 * @return {null}
 */
const sessionRetrieval = (dbType) => {
  let sessionStorageArr;
  fetch('/session', {
    method: 'GET',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }
  })
  .then(async function (res) {
    if (res.ok) {
      let sessionInfoResp = await res.json();
      sessionStorageArr = JSON.parse(sessionStorage.getItem('sessionStorage'));
      sessionInfoResp.sourceDbType = dbType;
      if (sessionStorageArr === null) {
        sessionStorageArr = [];
        sessionStorageArr.push(sessionInfoResp);
      }
      else {
        sessionStorageArr.push(sessionInfoResp);
      }
      sessionStorage.setItem('sessionStorage', JSON.stringify(sessionStorageArr));
    }
    else {
      sessionInfoResp = Promise.reject(res);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  })
}

/**
 * Function to store db dump values in local storage
 *
 * @param {string} dbType selected db like mysql, postgres, etc
 * @param {string} filePath path entered for the dump file
 * @return {null}
 */
const storeDumpFileValues = (dbType, filePath) => {
  let sourceTableFlag = '';
  if (dbType === 'mysql') {
    localStorage.setItem('globalDbType', dbType + 'dump');
    sourceTableFlag = 'MySQL';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
  else if (dbType === 'postgres') {
    localStorage.setItem('globalDbType', 'pg_dump');
    sourceTableFlag = 'Postgres';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
  localStorage.setItem('globalDumpFilePath', filePath);
  onLoadDatabase(localStorage.getItem('globalDbType'), localStorage.getItem('globalDumpFilePath'));
}

/**
 * Function to call /convert/dump api to get con json structure
 *
 * @param {string} dbType selected db like mysql, postgres, etc
 * @param {string} dumpFilePath path entered for the dump file
 * @return {null}
 */
const onLoadDatabase = async(dbType, dumpFilePath) => {
  let reportData, sourceTableFlag, reportDataResp;
  showSpinner();
  reportData = await fetch('/convert/dump', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      "Driver": dbType,
      "Path": dumpFilePath
    })
  });
  requestCode = await reportData.status;
  reportDataResp = await reportData.text();

  if (requestCode != 200) {
    hideSpinner();
    showSnackbar(reportDataResp, ' redBg');
    return;
  }
  else {
    window.location.href = '#/schema-report-load-db-dump';
    jQuery('#loadDatabaseDumpModal').modal('hide');
    reportDataResp = JSON.parse(reportDataResp);
    localStorage.setItem('conversionReportContent', JSON.stringify(reportDataResp));
  }
  await ddlSummaryAndConversionApiCall();
  await getInterleaveInfo();
  sourceTableFlag = localStorage.getItem('sourceDbName');
  const { component = ErrorComponent } = findComponentByPath('/schema-report-load-db-dump', routes) || {};
  if (document.getElementById('app')) {
    document.getElementById('app').innerHTML = component.render();
  }
  showSchemaConversionReportContent();
  sessionRetrieval(sourceTableFlag);
  showSnackbar('schema converted successfully !!', ' greenBg');
}

/**
 * Function to get interleave info for each table
 *
 * @return {null}
 */
const getInterleaveInfo = async() => {
  let schemaObj = JSON.parse(localStorage.getItem('conversionReportContent'));
  let tablesNumber = Object.keys(schemaObj.SpSchema).length;
  let interleaveApiCallResp = [];
  for (var i = 0; i < tablesNumber; i++) {
    let tableName = Object.keys(schemaObj.ToSpanner)[i];
    interleaveApiCall = await fetch('/checkinterleave/table?table=' + tableName)
    .then(async function (response) {
      if (response.ok) {
        return response;
      }
      else {
        return Promise.reject(response);
      }
    })
    .catch(function (err) {
      showSnackbar(err, ' redBg');
    });
    interleaveApiCallResp[i] = await interleaveApiCall.json();
  }
  localStorage.setItem('interleaveInfo', JSON.stringify(interleaveApiCallResp));
}

/**
 * Function to call database connection api.
 *
 * @param {string} dbType Type of db like mysql, postgres, etc
 * @param {string} dbHost Database host
 * @param {number} dbPort Database port number
 * @param {string} dbUser Database user name
 * @param {string} dbName Database name
 * @param {string} dbPassword Database password
 * @return {null}
 */
const onconnect = (dbType, dbHost, dbPort, dbUser, dbName, dbPassword) => {
  let sourceTableFlag = '';
  showSpinner();
  fetch('/connect', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      "Driver": dbType,
      "Database": dbName,
      "Password": dbPassword,
      "User": dbUser,
      "Port": dbPort,
      "Host": dbHost
    })
  })
  .then(function (res) {
    hideSpinner();
    if (res.ok) {
      sourceTableFlag = 'MySQL';
      localStorage.setItem('sourceDbName', sourceTableFlag);
      jQuery('#connectToDbModal').modal('hide');
      jQuery('#connectModalSuccess').modal();
      
    }
    else {
      res.text().then(function (result) {
        jQuery('#connectToDbModal').modal('hide');
        jQuery('#connectModalFailure').modal();
      });
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  })
}

/**
 * Function to import schema and populate summary, ddl, conversion report panels
 *
 * @return {null}
 */
const onImport = async() => {
  let driver = '';
  let srcDb = localStorage.getItem('sourceDbName');
  if (srcDb === 'MySQL') {
    driver = 'mysqldump';
  }
  else if (srcDb === 'Postgres') {
    driver = 'pg_dump';
  }
  let path = localStorage.getItem('importFilePath');
  let fileName = localStorage.getItem('importFileName');
  await fetch('/session/resume', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      "driver": driver,
      "path": path,
      "fileName": fileName
    })
  })
  .then(function(res) {
    console.log(res);
  });
  await ddlSummaryAndConversionApiCall();
  await getInterleaveInfo();
  jQuery('#importSchemaModal').modal('hide');
  const { component = ErrorComponent } = findComponentByPath('/schema-report-import-db', routes) || {};
  if (document.getElementById('app')) {
    document.getElementById('app').innerHTML = component.render();
  }
  showSchemaConversionReportContent();
}

/**
 * Function to store session info
 *
 * @param {string} driver database driver
 * @param {string} path file path
 * @param {string} fileName file name
 * @param {string} sourceDb source db name
 * @return {null}
 */
const storeResumeSessionId = (driver, path, fileName, sourceDb) => {
  localStorage.setItem('driver', driver);
  localStorage.setItem('path', path);
  localStorage.setItem('fileName', fileName);
  localStorage.setItem('sourceDb', sourceDb);
}

/**
 * Function to read file content when clicked on resume session
 *
 * @param {string} driver database driver
 * @param {string} path file path
 * @param {string} fileName file name
 * @param {string} sourceDb source db name
 * @param {string} windowEvent hashchange or load event
 * @return {null}
 */
const resumeSession = async(driver, path, fileName, sourceDb, windowEvent) => {
  let filePath = './' + fileName;
  let sourceTableFlag = '';
  readTextFile(filePath, function (text) {
    var data = JSON.parse(text);
    localStorage.setItem('conversionReportContent', JSON.stringify(data));
    sourceTableFlag = sourceDb;
  });
  await fetch('/session/resume', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      "driver": driver,
      "path": path,
      "fileName": fileName
    })
  })
  .then(function (response) {
    if (response.ok) {
      console.log(response);
    }
    else {
      Promise.reject(response);
    }
  })
  .catch(function (err) {
    showSnackbar(err, ' redBg');
  });
  await ddlSummaryAndConversionApiCall();
  await getInterleaveInfo();
  jQuery('#importSchemaModal').modal('hide');
  const { component = ErrorComponent } = findComponentByPath('/schema-report-resume-session', routes) || {};
  if (document.getElementById('app')) {
    document.getElementById('app').innerHTML = component.render();
  }
  showSchemaConversionReportContent();
  if (windowEvent === 'hashchange') {
    showSnackbar('schema resumed successfully', ' greenBg');
  }
}

/**
 * Callback function to read file content
 *
 * @param {file}
 * @return {null}
 */
const readTextFile = (file, callback) => {
  let rawFile = new XMLHttpRequest();
  rawFile.overrideMimeType("application/json");
  rawFile.open("GET", file, true);
  rawFile.onreadystatechange = function () {
    if (rawFile.readyState == 4 && rawFile.status == "200") {
      callback(rawFile.responseText);
    }
  }
  rawFile.send(null);
}

/**
 * Function to create session table content
 *
 * @return {null}
 */
const setSessionTableContent = () => {
  let sessionArray = JSON.parse(sessionStorage.getItem('sessionStorage'));
  let sessionContent = '';
  if (sessionArray === null) {
      sessionContent = `<tr>
                          <td colspan='5' class='center session-image'><img src='Icons/Icons/Group 2154.svg' alt='nothing to show'></td>
                        </tr>
                        <tr>
                          <td colspan='5' class='center simple-grey-text'>No active session available! <br> Please connect a database to initiate a new session.</td>
                        </tr>`;
  }
  else {
    let sessionArrayLength = sessionArray.length;
    for (var x = 0; x < sessionArrayLength; x++) {
      let session = sessionArray[x];
      let sessionName = session.fileName;
      let sessionDate = session.createdAt.substr(0, session.createdAt.indexOf("T"));
      let sessionTime = session.createdAt.substr(session.createdAt.indexOf("T") + 1);
      sessionContent += `<tr class='d-flex'>
                          <td class='col-2 session-table-td2'>${sessionName}</td>
                          <td class='col-4 session-table-td2'>${sessionDate}</td>
                          <td class='col-2 session-table-td2'>${sessionTime}</td>
                          <td class='col-4 session-table-td2 session-action' id=${x}>
                            <a href='#/schema-report-resume-session' style='cursor: pointer; text-decoration: none;' onclick='resumeSessionHandler(${x}, ${JSON.stringify(sessionArray)})'>Resume Session</a>
                          </td>
                        </tr>`;
    }
  }
  return sessionContent;
}

/**
 * Function to handle resume session click event
 *
 * @param {number} index session index in the array
 * @param {array} sessionArray array of objects containing session information
 * @return {null}
 */
const resumeSessionHandler = (index, sessionArray) => {
  storeResumeSessionId(sessionArray[index].driver, sessionArray[index].path, sessionArray[index].fileName, sessionArray[index].sourceDbType);
}

/**
 * Function to check source schema while importing any file
 *
 * @param {string} val source db value (mysql or postgres)
 * @return {null}
 */
const importSourceSchema = (val) => {
  let sourceTableFlag = '';
  if (val === 'mysql') {
    sourceTableFlag = 'MySQL';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
  else if (val === 'postgres') {
    sourceTableFlag = 'Postgres';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
}

/**
 * Function to get paths and events generated from window
 *
 * @param {object} params object containing path and event as keys
 * @return {null}
 */
const getComponent = (params) => {
  if (params.path === '/schema-report-connect-to-db' && params.event === 'hashchange') {
    showSchemaAssessment(window.event.type);
  }
  else if ( (params.path === '/schema-report-connect-to-db' || params.path === '/schema-report-load-db-dump') && params.event === 'load') {
    const { component = ErrorComponent } = findComponentByPath(location.hash.slice(1).toLowerCase() || '/', routes) || {};
    document.getElementById('app').innerHTML = component.render();
    conversionRateResp = JSON.parse(localStorage.getItem('tableBorderColor'));
    createSourceAndSpannerTables(JSON.parse(localStorage.getItem('conversionReportContent')));
    createDdlFromJson(JSON.parse(localStorage.getItem('ddlStatementsContent')));
    createSummaryFromJson(JSON.parse(localStorage.getItem('summaryReportContent')));
  }
  else if (params.path === '/schema-report-import-db') {
    onImport();
  }
  else if (params.path === '/schema-report-resume-session') {
    resumeSession(localStorage.getItem('driver'), localStorage.getItem('path'), localStorage.getItem('fileName'), localStorage.getItem('sourceDb'), window.event.type);
  }
  else {
    return false;
  }
  return true;
}

/**
 * Function to render home screen html and initiate home screen tasks
 *
 * @return {null}
 */
const homeScreen = () => {
  initHomeScreenTasks();
  return homeScreenHtml();
}

/**
 * Function to render home screen html
 *
 * @return {html}
 */
const homeScreenHtml = () => {
  return (`
  <div class="main-content">
    <h5 class="welcome-heading">
      Welcome To HarbourBridge
    </h5>
    <h5 class="connect-heading">
      Connect or import your database
    </h5>
    <div class="card-section">
      <div class="card-alignment">
        <div class="card-1-alignment">
          <div class="mdc-card connect-db-icon pointer" data-toggle="modal" data-target="#connectToDbModal" data-backdrop="static" data-keyboard="false">
            <img src="Icons/Icons/Group 2048.svg" width="64" height="64"  style="margin:auto" alt="connect to db">
          </div>
          <div class="connect-text pointer" data-toggle="modal" data-target="#connectToDbModal" data-backdrop="static" data-keyboard="false">
              Connect to Database
          </div>
        </div>

        <div class="card-2-alignment">
          <div class="mdc-card connect-db-icon pointer" data-toggle="modal" data-target="#loadDatabaseDumpModal" data-backdrop="static" data-keyboard="false">
            <img src="Icons/Icons/Group 2049.svg" width="64" height="64" style="margin:auto"  alt="load database image">
          </div>
          <div class="load-text pointer" data-toggle="modal" data-target="#loadDatabaseDumpModal" data-backdrop="static" data-keyboard="false">
              Load Database Dump
            </div>
        </div>

        <div class="card-3-alignment">
          <div class="mdc-card connect-db-icon pointer" data-toggle="modal" data-target="#importSchemaModal" data-backdrop="static" data-keyboard="false">
            <img src="Icons/Icons/Group 2047.svg" width="64" height="64" style="margin:auto"  alt="import schema image">
          </div>
          <div class="import-text pointer" data-toggle="modal" data-target="#importSchemaModal" data-backdrop="static" data-keyboard="false">
              Import Schema File
          </div>
        </div>
      </div>
    </div>

    <div id="snackbar"></div>
    <div class='spinner-backdrop' id='toggle-spinner' style="display:none">
      <div id="spinner"></div>
    </div>
    <h4 class="session-heading">Conversion history</h4>

    <table class="table session-table" style="width: 95%;">
      <thead>
        <tr class="d-flex">
          <th class='col-2 session-table-th2'>Session Name</th>
          <th class='col-4 session-table-th2'>Date</th>
          <th class='col-2 session-table-th2'>Time</th>
          <th class='col-4 session-table-th2'>Action Item</th>
        </tr>
      </thead>
      <tbody id='session-table-content'>
        ${setSessionTableContent()}
      </tbody>
    </table>
  </div>

  <!-- Connect to Db Modal -->
  <div class="modal" id="connectToDbModal" tabindex="-1" role="dialog" aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered" role="document">
      <div class="modal-content">
        <div class="modal-header content-center">
          <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Connect to Database</h5>
          <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
        </div>
        <div class="modal-body">
          <div class="form-group">
            <label for="dbType" class="">Database Type</label>
            <select class="form-control db-select-input" id="dbType" name="dbType" onchange="toggleDbType()">
              <option value="" style="display: none;"></option>
              <option class="db-option" value="mysql">MySQL</option>
              <option class="db-option" value="postgres">Postgres</option>
              <option class='db-option' value='dynamodb'>dynamoDB</option>
            </select>
          </div>
          <div id="sqlFields" style="display: none;">
              <form id="connectForm">
                <div class="form-group">
                  <label class="modal-label" for="dbHost">Database Host</label>
                  <input type="text" class="form-control db-input" aria-describedby="" name="dbHost" id="dbHost" autocomplete="off" onfocusout="validateInput(document.getElementById('dbHost'), 'dbHostError')"/>
                  <span class='formError' id='dbHostError'></span><br>
                </div>

                <div class="form-group">
                  <label class="modal-label" for="dbPort">Database Port</label>
                  <input class="form-control db-input" aria-describedby="" type="text" name="dbPort" id="dbPort" autocomplete="off" onfocusout="validateInput(document.getElementById('dbPort'), 'dbPortError')"/>
                  <span class='formError' id='dbPortError'></span><br>
                </div>

                <div class="form-group">
                  <label class="modal-label" for="dbUser">Database User</label>
                  <input class="form-control db-input" aria-describedby="" type="text" name="dbUser" id="dbUser" autocomplete="off" onfocusout="validateInput(document.getElementById('dbUser'), 'dbUserError')"/>
                  <span class='formError' id='dbUserError'></span><br>
                </div>

                <div class="form-group">
                  <label class="modal-label" for="dbName">Database Name</label>
                  <input class="form-control db-input" aria-describedby="" type="text" name="dbName" id="dbName" autocomplete="off" onfocusout="validateInput(document.getElementById('dbName'), 'dbNameError')"/>
                  <span class='formError' id='dbNameError'></span><br>
                </div>

                <div class="form-group">
                  <label class="modal-label" for="dbPassword">Database Password</label>
                  <input class="form-control db-input" aria-describedby="" type="password" name="dbPassword" id="dbPassword" autocomplete="off" onfocusout="validateInput(document.getElementById('dbPassword'), 'dbPassError')"/>
                  <span class='formError' id='dbPassError'></span><br>
                </div>
              </form>
            </div>
          </div>
        <div id="sqlFieldsButtons" style="display: none;">
          <div class="modal-footer">
            <input type="submit" disabled="disabled" value="Connect" id="connectButton" class="connectButton" 
            onclick="onconnect( document.getElementById('dbType').value, document.getElementById('dbHost').value, document.getElementById('dbPort').value, document.getElementById('dbUser').value, document.getElementById('dbName').value, document.getElementById('dbPassword').value)" />
          </div>
        </div>
      </div>
    </div>
  </div>


  <!-- Load Database Dump Modal -->
  <div class="modal loadDatabaseDumpModal" id="loadDatabaseDumpModal" tabindex="-1" role="dialog" aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered" role="document">

      <!-- Modal content-->
      <div class="modal-content">
        <div class="modal-header content-center">
          <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Load Database Dump</h5>
          <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
        </div>
        <div class="modal-body">
          <!-- <form id="loadDbForm"> -->
          <div class="form-group">
            <label class="" for="loadDbType">Database Type</label>
              <select class="form-control load-db-input" id="loadDbType" name="loadDbType">
                <option value="" style="display: none;"></option>
                <option class="db-option" value="mysql">MySQL</option>
                <option class="db-option" value="postgres">Postgres</option>
              </select>
          </div>

          <form id="loadDbForm">
            <div class="form-group">
              <label class="modal-label" for="dumpFilePath">Path of the Dump File</label>
              <input class="form-control load-db-input" aria-describedby="" type="text" name="dumpFilePath" id="dumpFilePath" autocomplete="off" onfocusout="validateInput(document.getElementById('dumpFilePath'), 'filePathError')"/>
              <span class='formError' id='filePathError'></span>
            </div>
            <input type="text" style="display: none;">
          </form>
        </div>
        <div class="modal-footer">
          <input type="submit" disabled='disabled' value='Confirm' id='loadConnectButton' class='connectButton' onclick='storeDumpFileValues(document.getElementById("loadDbType").value, document.getElementById("dumpFilePath").value)'/>
        </div>
      </div>
    </div>
  </div>

  <!-- Import Schema Modal -->
  <div class="modal importSchemaModal" id="importSchemaModal" tabindex="-1" role="dialog" aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered" role="document">
      <!-- Modal content-->
      <div class="modal-content">
        <div class="modal-header content-center">
          <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Import Schema File</h5>
          <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
        </div>
        <div class="modal-body">

          <form id="importForm" class="importForm">
          <div class="form-group">
          <label class="modal-label" for="importDbType">Database Type</label>
          <select class="form-control import-db-input" id="importDbType" name="importDbType" >
            <option value="" style="display: none;"></option>
            <option class="db-option" value="mysql">MySQL</option>
            <option class="db-option" value="postgres">Postgres</option>
          </select>
          </div>

          <div class="form-group">
              <label class="modal-label" for="schemaFile">Schema File</label><br>
              <input class="form-control" aria-describedby="" id="upload" type="file" onchange='filenameChangeHandler(event)'/>
              <a href="" id="upload_link" onclick='uploadFileHandler(event)'>Upload File</a>​
          </div>
          </form>
        </div>
        <div class="modal-footer">
          <a href='#/schema-report-import-db'><input type='submit' disabled='disabled' id='importButton' class='connectButton' value='Confirm' onclick='importSourceSchema(document.getElementById("importDbType").value)'/></a>
        </div>
      </div>
    </div>
  </div>
 
  <div class="modal" id="connectModalSuccess" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle" aria-hidden="true" data-backdrop="static" data-keyboard="false">
    <div class="modal-dialog modal-dialog-centered" role="document">
      <!-- Modal content-->
      <div class="modal-content">
        <div class="modal-header content-center">
          <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Connection Successful</h5>
          <i class="large material-icons close" data-dismiss="modal" onclick='clearModal()'>cancel</i>
        </div>
        <div class="modal-body" style='margin-bottom: 20px; display: inherit;'>
          <div><i class="large material-icons connectionSuccess">check_circle</i></div>
          <div>Please click on convert button to proceed with schema conversion</div>
        </div>
        <div class="modal-footer">
          <a href='#/schema-report-connect-to-db'><button id="convert-button" class="connectButton" type="button">Convert</button></a>
          <button class="buttonload" id="convertLoaderButton" style="display: none;">
              <i class="fa fa-circle-o-notch fa-spin"></i>converting
          </button>
        </div>
      </div>
    </div>
  </div>

  <div class="modal" id="connectModalFailure" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle" aria-hidden="true" data-backdrop="static" data-keyboard="false">
    <div class="modal-dialog modal-dialog-centered" role="document">
      <!-- Modal content-->
      <div class="modal-content">
        <div class="modal-header content-center">
          <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Connection Failure</h5>
          <i class="large material-icons close" data-dismiss="modal" onclick='clearModal()'>cancel</i>
        </div>
        <div class="modal-body" style='margin-bottom: 20px; display: inherit;'>
            <div><i class="large material-icons connectionFailure">cancel</i></div>
            <div>Please check database configuration details and try again !!</div>
        </div>
        <div class="modal-footer">
          <button data-dismiss="modal" onclick='clearModal()' class="connectButton" type="button">Ok</button>
        </div>
      </div>
    </div>
  </div>`
  )
}