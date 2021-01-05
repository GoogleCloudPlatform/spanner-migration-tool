// variable declarations
var notFoundTxt = document.createElement('h5');
notFoundTxt.innerHTML = `No Match Found`;
notFoundTxt.className = 'noText';
notFoundTxt.style.display = 'none';
var tableListArea = 'accordion';

/**
 * Function to initialise the tasks of edit schema screen
 *
 * @return {null}
 */
const initTasks = () => {
  var reportAccCount = 0;
  var summaryAccCount = 0;
  var ddlAccCount = 0;
  jQuery(document).ready(() => {
    setActiveSelectedMenu('schemaScreen');
    jQuery('.reportCollapse').on('show.bs.collapse', function() {
      jQuery(this).closest('.card').find('.rotate-icon').toggleClass('down');
      reportAccCount = reportAccCount + 1;
      document.getElementById('reportExpandButton').innerHTML = 'Collapse All';
    });

    jQuery('.reportCollapse').on('hide.bs.collapse', function() {
      jQuery(this).closest('.card').find('.rotate-icon').toggleClass('down');
      reportAccCount = reportAccCount - 1;
      if (reportAccCount === 0) {
        document.getElementById('reportExpandButton').innerHTML = 'Expand All';
      }
    });

    jQuery('.ddlCollapse').on('show.bs.collapse', function() {
      jQuery(this).closest('.card').find('.rotate-icon').toggleClass('down');
      ddlAccCount = ddlAccCount + 1;
      document.getElementById('ddlExpandButton').innerHTML = 'Collapse All';
    })

    jQuery('.ddlCollapse').on('hide.bs.collapse', function() {
      jQuery(this).closest('.card').find('.rotate-icon').toggleClass('down');
      ddlAccCount = ddlAccCount - 1;
      if (ddlAccCount === 0) {
        document.getElementById('ddlExpandButton').innerHTML = 'Expand All';
      }
    })

    jQuery('.summaryCollapse').on('show.bs.collapse', function() {
      jQuery(this).closest('.card').find('.rotate-icon').toggleClass('down');
      summaryAccCount = summaryAccCount + 1;
      document.getElementById('summaryExpandButton').innerHTML = 'Collapse All';
    })

    jQuery('.summaryCollapse').on('hide.bs.collapse', function() {
      jQuery(this).closest('.card').find('.rotate-icon').toggleClass('down');
      summaryAccCount = summaryAccCount - 1;
      if (summaryAccCount === 0) {
        document.getElementById('summaryExpandButton').innerHTML = 'Expand All';
      }
    })

    jQuery('.collapse').on('show.bs.collapse hide.bs.collapse', function() {
      jQuery(this).closest('.card').find('.card-header .right-align').toggleClass('show-content hide-content');
      jQuery(this).closest('.card').find('.report-card-header').toggleClass('borderBottom remBorderBottom');
      jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
    });
  });
}

/**
 * Function for calling initTasks and html functions for edit schema screen
 *
 * @return {Function}
 */
const schemaReport = () => {
  initTasks();
  return renderSchemaReportHtml();
}

/**
 * Function to implement search functionality in all the tabs of edit schema screen
 *
 * @return {null}
 */
const searchTable = (tabId) => {
  let searchInput, searchInputFilter, tableVal, list, listElem, elem;
  let flag = false;
  elem = document.getElementById('tabBg');
  if (elem) {
    elem.appendChild(notFoundTxt);
  }
  notFoundTxt.style.display = 'none';
  searchInput = document.getElementById(tabId);
  if (searchInput) {
    searchInputFilter = searchInput.value.toUpperCase();
  }
  list = document.getElementById(tableListArea);
  if (list) {
    list.style.display = ''; 
  }
  listElem = list.getElementsByTagName('section');
  tableListLength = Object.keys(schemaConversionObj.SpSchema).length;
  for (var i = 0; i < Object.keys(schemaConversionObj.SpSchema).length; i++) {
    tableVal = Object.keys(schemaConversionObj.SpSchema)[i];
    if (tableVal.toUpperCase().indexOf(searchInputFilter) > -1) {
      listElem[i].style.display = '';
      flag = true;
    }
    else {
      listElem[i].style.display = 'none';
    }
  }

  if (flag === false) {
    notFoundTxt.style.display = '';
    list.style.display = 'none';
  }

}

/**
 * Function to call set data type api
 *
 * @return {null}
 */
const setGlobalDataType = () => {
  var dataTypeJson = {};
  var tableLen = jQuery('#globalDataTypeTable tr').length;
  for (var i = 1; i < tableLen; i++) {
    var row = document.getElementById('dataTypeRow' + i);
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
  fetch('/typemap/global', {
    method: 'POST',
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(dataTypeJson)
  })
    .then(function (res) {
      res.json().then(async function (response) {
        localStorage.setItem('conversionReportContent', JSON.stringify(response));
        await ddlSummaryAndConversionApiCall();
        const { component = ErrorComponent } = findComponentByPath(location.hash.slice(1).toLowerCase() || paths.defaultPath, routes) || {};
        document.getElementById('app').innerHTML = component.render();
        showSchemaConversionReportContent();
      });
    })
}

/**
 * Function to download schema report
 *
 * @return {null}
 */
const downloadSchema = () => {
  let downloadFilePaths = JSON.parse(localStorage.getItem('downloadFilePaths'));
  let schemaFilePath = downloadFilePaths.Schema;
  let schemaFileName = schemaFilePath.split('/')[schemaFilePath.split('/').length - 1];
  let filePath = './' + schemaFileName;
  readTextFile(filePath, function (text) {
    jQuery("<a />", {
      "download": schemaFileName + ".txt",
      "href": "data:application/json;charset=utf-8," + encodeURIComponent(text),
    }).appendTo("body")
    .click(function () {
      jQuery(this).remove()
    })[0].click()
  });
}

/**
 * Function to download ddl statements
 *
 * @return {null}
 */
const downloadDdl = () => {
  jQuery("<a />", {
    "download": "ddl.json",
    "href": "data:application/json;charset=utf-8," + encodeURIComponent(JSON.stringify(JSON.parse(localStorage.getItem('ddlStatementsContent')), null, 4)),
  }).appendTo("body")
    .click(function () {
      jQuery(this).remove()
    })[0].click();
}

/**
 * Function to download summary report
 *
 * @return {null}
 */
const downloadReport = () => {
  let downloadFilePaths = JSON.parse(localStorage.getItem('downloadFilePaths'));
  let reportFilePath = downloadFilePaths.Report;
  let reportFileName = reportFilePath.split('/')[reportFilePath.split('/').length - 1];
  let filePath = './' + reportFileName;
  readTextFile(filePath, function (text) {
    jQuery("<a />", {
      "download": reportFileName + '.txt',
      "href": "data:application/json;charset=utf-8," + encodeURIComponent(text),
    }).appendTo("body")
    .click(function () {
      jQuery(this).remove()
    })[0].click();
  })
}

/**
 * Function to handle click event on expand all button of report tab
 *
 * @return {null}
 */
const reportExpandHandler = (event) => {
  if (event[0].innerText === 'Expand All') {
    event[0].innerText = 'Collapse All';
    jQuery('.reportCollapse').collapse('show');
  }
  else {
    event[0].innerText = 'Expand All';
    jQuery('.reportCollapse').collapse('hide');
  }
}

/**
 * Function to handle click event on expand all button of ddl tab
 *
 * @return {null}
 */
const ddlExpandHandler = (event) => {
  if (event[0].innerText === 'Expand All') {
    event[0].innerText = 'Collapse All';
    jQuery('.ddlCollapse').collapse('show');
  }
  else {
    event[0].innerText = 'Expand All';
    jQuery('.ddlCollapse').collapse('hide');
  }
}

/**
 * Function to handle click event on expand all button of summary tab
 *
 * @return {null}
 */
const summaryExpandHandler = (event) => {
  if (event[0].innerText === 'Expand All') {
    event[0].innerText = 'Collapse All';
    jQuery('.summaryCollapse').collapse('show');
  }
  else {
    event[0].innerText = 'Expand All';
    jQuery('.summaryCollapse').collapse('hide');
  }
}

/**
 * Function to handle click event on edit global data type button of report tab
 *
 * @return {null}
 */
const globalEditHandler = () => {
  createEditDataTypeTable();
  jQuery('#globalDataTypeModal').modal();
}

/**
 * Function to create global edit data type table
 *
 * @return {null}
 */
const createEditDataTypeTable = () => {
  let globalDataTypeList = JSON.parse(localStorage.getItem('globalDataTypeList'));
  let dataTypeListLength = Object.keys(globalDataTypeList).length;
  let tableContent = '';
  let globalDataTypeTable = '';

  for (var i = 0; i < dataTypeListLength; i++) {
    tableContent += `<tr id='dataTypeRow${(i + 1)}'>`;
    for (var j = 0; j < 2; j++) {
      if (globalDataTypeList[Object.keys(globalDataTypeList)[i]] !== null)
      {
        if (j === 0) {
          tableContent += `<td class='src-td' id='dataTypeKey${(i + 1)}'>${Object.keys(globalDataTypeList)[i]}</td>`;
        }
        else if (j === 1) {
          tableContent += `<td id='dataTypeVal${(i + 1)}'>`
          let selectHTML = '';
          let selectId = 'dataTypeOption' + (i + 1);
          let optionsLength = globalDataTypeList[Object.keys(globalDataTypeList)[i]].length;
          selectHTML = `<div style='display: flex;'>`;
          if (globalDataTypeList[Object.keys(globalDataTypeList)[i]][0].Brief !== "") {
            selectHTML += `<i class="large material-icons warning" style='cursor: pointer;' data-toggle='tooltip' data-placement='bottom' title='${globalDataTypeList[Object.keys(globalDataTypeList)[i]][0].Brief}'>warning</i>`;
          }
          else {
            selectHTML += `<i class="large material-icons warning" style='cursor: pointer; visibility: hidden;'>warning</i>`
          }
          selectHTML += `<select onchange='dataTypeUpdate(id, ${JSON.stringify(globalDataTypeList)})' class='form-control tableSelect' id=${selectId} style='border: 0px !important;'>`
          for (var k = 0; k < optionsLength; k++) {
            selectHTML += `<option value='${globalDataTypeList[Object.keys(globalDataTypeList)[i]][k].T}'>${globalDataTypeList[Object.keys(globalDataTypeList)[i]][k].T} </option>`;
          }
          selectHTML += `</select></div>`;
          tableContent += selectHTML + `</td>`;
        }
      }
    }
    tableContent += `</tr>`;
  }

  globalDataTypeTable = `<table class='data-type-table' id='globalDataTypeTable'>
                            <tbody>
                              <tr>
                                <th>Source</th>
                                <th>Spanner</th>
                              </tr>
                              ${tableContent}
                            </tbody>
                         </table>`;
  document.getElementById('globalDataType').innerHTML = globalDataTypeTable;
  tooltipHandler();
}

/**
 * Function to update data types with warnings(if any) in global data type table
 *
 * @param {string} id id of select box in global data type table
 * @return {null}
 */
const dataTypeUpdate = (id, globalDataTypeList) => {
  let idNum = parseInt(id.match(/\d+/), 10);
  let dataTypeOptionArray = globalDataTypeList[document.getElementById('dataTypeKey' + idNum).innerHTML];
  let optionHTML = '';
  let selectHTML = '';
  let optionFound;
  let warningFound;
  let length = dataTypeOptionArray.length;
  warningFound = `<i class="large material-icons warning" style='cursor: pointer; visibility: hidden;'>warning</i>`;
  for (var x = 0; x < length; x++) {
    optionFound = dataTypeOptionArray[x].T === document.getElementById(id).value;
    if (dataTypeOptionArray[x].T === document.getElementById(id).value && dataTypeOptionArray[x].Brief !== "") {
      warningFound = `<i class="large material-icons warning" style='cursor: pointer;' data-toggle='tooltip' data-placement='bottom' title='${dataTypeOptionArray[x].Brief}'>warning</i>`;
    }
    if (optionFound === true) {
      optionHTML += `<option selected='selected' value='${dataTypeOptionArray[x].T}'>${dataTypeOptionArray[x].T} </option>`;
    }
    else {
      optionHTML += `<option value='${dataTypeOptionArray[x].T}'>${dataTypeOptionArray[x].T} </option>`;
    }
  }
  selectHTML = `<div style='display: flex;'>` + warningFound + `<select onchange='dataTypeUpdate(id, ${JSON.stringify(globalDataTypeList)})' class='form-control tableSelect' id=${id} style='border: 0px !important;'>`;
  selectHTML += optionHTML + `</select></div>`;
  let dataTypeValEle = document.getElementById('dataTypeVal' + idNum);
  if (dataTypeValEle) {
    document.getElementById('dataTypeVal' + idNum).innerHTML = selectHTML;
  }
  tooltipHandler();
}

/**
 * Function to render edit schema screen html
 *
 * @return {html}
 */
const renderSchemaReportHtml = () => {
  currentLocation = "#" + location.hash.slice(1).toLowerCase() || paths.defaultPath;
  return (`
        <div id="snackbar"></div>

        <div class='spinner-backdrop' id='toggle-spinner'>
          <div id="spinner"></div>
        </div>

        <div class="summary-main-content">

            <div>
                <h4 class="report-header">Recommended Schema Conversion Report 
                  <button id="download-schema" class="download-button" onclick='downloadSchema()'>Download Schema File</button>
                  <button id="download-ddl" style='display: none;' class="download-button" onclick='downloadDdl()'>Download SQL Schema</button>
                  <button id="download-report" style='display: none;' class="download-button" onclick='downloadReport()'>Download Report</button>
                </h4>
            </div>
            <div class="report-tabs">
              <ul class="nav nav-tabs md-tabs" role="tablist">
                <li class="nav-item">
                  <a class="nav-link active" id="reportTab" data-toggle="tab" href="#report" role="tab" aria-controls="report"
                    aria-selected="true" onclick='findTab(this.id)'>Conversion Report</a>
                </li>
                <li class="nav-item">
                  <a class="nav-link" id="ddlTab" data-toggle="tab" href="#ddl" role="tab" aria-controls="ddl"
                    aria-selected="false" onclick='findTab(this.id)'>DDL Statements</a>
                </li>
                <li class="nav-item">
                  <a class="nav-link" id="summaryTab" data-toggle="tab" href="#summary" role="tab" aria-controls="summary"
                    aria-selected="false" onclick='findTab(this.id)'>Summary Report</a>
                </li>
              </ul>
            </div>

            <div class="status-icons">

              <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm" id='reportSearchForm'>
                <i class="fas fa-search" aria-hidden="true"></i>
                <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table" autocomplete='off'
                  aria-label="Search" onkeyup='searchTable("reportSearchInput")' id='reportSearchInput'>
              </form>

              <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm" style='display: none !important;' id='ddlSearchForm'>
                <i class="fas fa-search" aria-hidden="true"></i>
                <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table" id='ddlSearchInput' autocomplete='off'
                  aria-label="Search" onkeyup='searchTable("ddlSearchInput")'>
              </form>

              <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm" style='display: none !important;' id='summarySearchForm'>
                <i class="fas fa-search" aria-hidden="true"></i>
                <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table" id='summarySearchInput' autocomplete='off'
                  aria-label="Search" onkeyup='searchTable("summarySearchInput")'>
              </form>

              <section class="cus-tip">
                <span  class="cus-a info-icon statusTooltip">
                  <i class="large material-icons">info</i>
                  <span class="legend-icon statusTooltip" style='cursor: pointer;display: inline-block;vertical-align: super;'>Status&nbsp;&nbsp;Legend</span>
                </span>
                <div class="legend-hover">
                    <div class="legend-status">
                      <span class="excellent"></span>
                      Excellent
                    </div>
                    <div class="legend-status"> 
                      <span class="good"></span>
                      Good
                    </div>
                    <div class="legend-status">
                      <span class="poor"></span>
                      Poor
                    </div>
                </div>
              </section>


              </div>
            
            <div class="tab-bg" id='tabBg'>
            <div class="tab-content"> 

                <div id="report" class="tab-pane fade show active">


                  <div class="accordion md-accordion" id="accordion" role="tablist" aria-multiselectable="true">
                    <button class='expand' id='reportExpandButton' onclick='reportExpandHandler(jQuery(this))'>Expand All</button>
                    <button class='expand right-align' id='editButton' onclick='globalEditHandler()'>Edit Global Data Type</button>
                  </div>

                </div>

                <div id="ddl" class="tab-pane fade">
                    <div class="panel-group" id="ddl-accordion">
                      <button class='expand' id='ddlExpandButton' onclick='ddlExpandHandler(jQuery(this))'>Expand All</button>
                    </div> 
                </div>

                <div id="summary" class="tab-pane fade">
                    <div class="panel-group" id="summary-accordion">
                      <button class='expand' id='summaryExpandButton' onclick='summaryExpandHandler(jQuery(this))'>Expand All</button>
                    </div> 
                </div>

            </div>
                
            </div>
          </div>
        </div>


        <div class="modal" id="globalDataTypeModal" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle" aria-hidden="true" data-backdrop="static" data-keyboard="false">
        <div class="modal-dialog modal-dialog-centered" role="document">
          <!-- Modal content-->
          <div class="modal-content">
            <div class="modal-header content-center">
              <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Global Data Type Mapping</h5>
              <i class="large material-icons close" data-dismiss="modal">cancel</i>
            </div>
            <div class="modal-body" style='margin: auto; margin-top: 20px;'>
      
              <div class="dataMappingCard" id='globalDataType'>
                
              </div>
              
            </div>
            <div class="modal-footer" style='margin-top: 20px;'>
              <button id="data-type-button" data-dismiss="modal" onclick="setGlobalDataType()" class="connectButton" type="button" style='margin-right: 24px !important;'>Next</button>
              <button class="buttonload" id="dataTypeLoaderButton" style="display: none;">
                  <i class="fa fa-circle-o-notch fa-spin"></i>converting
              </button>
            </div>
          </div>
      
        </div>    
    `);

}

