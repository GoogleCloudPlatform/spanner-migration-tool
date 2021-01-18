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
const initSchemaScreenTasks = () => {
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
 * Function to implement search functionality in all the tabs of edit schema screen
 *
 * @param {string} tabId html id attriute for report, ddl or summary tabs
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
  for (var i = 0; i < tableListLength; i++) {
    tableVal = Object.keys(schemaConversionObj.SpSchema)[i];
    if (tableVal.toUpperCase().indexOf(searchInputFilter) > -1) {
      listElem[i+1].style.display = '';
      flag = true;
    }
    else {
      listElem[i+1].style.display = 'none';
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
  let globalDataTypeList = JSON.parse(localStorage.getItem('globalDataTypeList'));
  let dataTypeListLength = Object.keys(globalDataTypeList).length;
  let dataTypeJson = {};
  for (var i = 0; i < dataTypeListLength; i++) {
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
      component.render();
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
  for (var i = 0; i < dataTypeListLength; i++) {
    if (document.getElementById('dataTypeRow' + (i+1)) !== null) {
      break
    }
    if (globalDataTypeList[Object.keys(globalDataTypeList)[i]] !== null) {
      let $dataTypeOption;
      let $dataTypeRow = jQuery('#globalDataTypeTable').find('.globalDataTypeRow.template').clone().removeClass('template');
      $dataTypeRow.attr('id', 'dataTypeRow' + (i + 1));
      for (var j = 0; j < 2; j++) {
        if (j === 0) {
          $dataTypeRow.find('.src-td').attr('id', 'dataTypeKey' + (i + 1));
          $dataTypeRow.find('.src-td').html(Object.keys(globalDataTypeList)[i]);
        }
        else if (j === 1) {
          $dataTypeRow.find('#globalDataTypeCell').attr('id', 'dataTypeVal' + (i + 1));
          let optionsLength = globalDataTypeList[Object.keys(globalDataTypeList)[i]].length;
          if (globalDataTypeList[Object.keys(globalDataTypeList)[i]][0].Brief !== "") {
            $dataTypeRow.find('i').attr('data-toggle', 'tooltip');
            $dataTypeRow.find('i').attr('data-placement', 'bottom');
            $dataTypeRow.find('i').attr('title', globalDataTypeList[Object.keys(globalDataTypeList)[i]][0].Brief);
          }
          else {
            $dataTypeRow.find('i').css('visibility', 'hidden');
          }
          $dataTypeRow.find('select').attr('id', 'dataTypeOption' + (i + 1));
          for (var k = 0; k < optionsLength; k++) {
            $dataTypeOption = $dataTypeRow.find('.dataTypeOption.template').clone().removeClass('template');
            $dataTypeOption.attr('value', globalDataTypeList[Object.keys(globalDataTypeList)[i]][k].T);
            $dataTypeOption.html(globalDataTypeList[Object.keys(globalDataTypeList)[i]][k].T);
            $dataTypeOption.appendTo($dataTypeRow.find('select'));
          }
        }
      }
      $dataTypeRow.find('select').find("option").eq(0).remove();
      $dataTypeRow.find('#dataTypeOption' + (i+1)).unbind('change').bind('change', function() {
        dataTypeUpdate(jQuery(this).attr('id'), globalDataTypeList);
      });
      $dataTypeRow.appendTo(jQuery('#globalDataTypeTable'));
    }
  }
  tooltipHandler();
}

/**
 * Function to update data types with warnings(if any) in global data type table
 *
 * @param {string} id id of select box in global data type table
 * @param {list} globalDataTypeList list for source and spanner global data types
 * @return {null}
 */
const dataTypeUpdate = (id, globalDataTypeList) => {
  let idNum = parseInt(id.match(/\d+/), 10);
  let dataTypeOptionArray = globalDataTypeList[document.getElementById('dataTypeKey' + idNum).innerHTML];
  let optionFound;
  let length = dataTypeOptionArray.length;
  let $dataTypeSel = jQuery('.globalDataTypeRow.template').clone();
  $dataTypeSel.find('.src-td').attr('id', 'dataTypeKey' + idNum);
  $dataTypeSel.find('.src-td').html(Object.keys(globalDataTypeList)[idNum-1]);
  $dataTypeSel.find('i').css('visibility', 'hidden');
  for (var x = 0; x < length; x++) {
    let $dataTypeOption = $dataTypeSel.find('.dataTypeOption.template').clone().removeClass('template');
    optionFound = dataTypeOptionArray[x].T === document.getElementById(id).value;
    if (dataTypeOptionArray[x].T === document.getElementById(id).value && dataTypeOptionArray[x].Brief !== "") {
      $dataTypeSel.find('i').attr('data-toggle', 'tooltip');
      $dataTypeSel.find('i').attr('data-placement', 'bottom');
      $dataTypeSel.find('i').attr('title', dataTypeOptionArray[x].Brief);
      $dataTypeSel.find('i').css('visibility', '');
    }
    if (optionFound === true) {
      $dataTypeOption.attr('value', dataTypeOptionArray[x].T);
      $dataTypeOption.html(dataTypeOptionArray[x].T);
      $dataTypeOption.attr('selected', 'selected');
    }
    else {
      $dataTypeOption.attr('value', dataTypeOptionArray[x].T);
      $dataTypeOption.html(dataTypeOptionArray[x].T);
    }
    $dataTypeOption.appendTo($dataTypeSel.find('select'));
  }
  $dataTypeSel.find('select').find("option").eq(0).remove();
  $dataTypeSel.find('select').attr('id', id);
  jQuery(this).unbind('change').bind('change', function() {
    dataTypeUpdate(id, globalDataTypeList);
  });
  jQuery("#dataTypeRow" + idNum).html($dataTypeSel.html());
  tooltipHandler();
}

/**
 * Function to render html for edit schema screen
 *
 * @return {null}
 */
const schemaReport = () => {
  jQuery('#app').load('./schema-conversion-screen.html');
}

