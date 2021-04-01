export const initSchemaScreenTasks = () => {
  var reportAccCount = 0;
  var summaryAccCount = 0;
  var ddlAccCount = 0;
  jQuery(document).ready(() => {
    setActiveSelectedMenu('schemaScreen');

    $(".modal-backdrop").hide();
    jQuery('.collapse.reportCollapse').on('show.bs.collapse', function () {
      if (!jQuery(this).closest('section').hasClass('template')) {
        jQuery(this).closest('.card').find('.rotate-icon').addClass('down');
        jQuery(this).closest('.card').find('.card-header .right-align').toggleClass('show-content hide-content');
        jQuery(this).closest('.card').find('.report-card-header').toggleClass('borderBottom remBorderBottom');
        reportAccCount = reportAccCount + 1;
        document.getElementById('reportExpandButton').innerHTML = 'Collapse All';
      }
    });
    jQuery('.collapse.reportCollapse').on('hide.bs.collapse', function () {
      if (!jQuery(this).closest('section').hasClass('template')) {
        jQuery(this).closest('.card').find('.rotate-icon').removeClass('down');
        jQuery(this).closest('.card').find('.card-header .right-align').toggleClass('show-content hide-content');
        jQuery(this).closest('.card').find('.report-card-header').toggleClass('borderBottom remBorderBottom');
        reportAccCount = reportAccCount - 1;
        if (reportAccCount === 0) {
          document.getElementById('reportExpandButton').innerHTML = 'Expand All';
        }
      }
    });

    jQuery('.collapse.innerSummaryCollapse').on('show.bs.collapse', function (e) {
      if (!jQuery(this).closest('section').hasClass('template')) {
        e.stopPropagation();
      }
    });
    jQuery('.collapse.innerSummaryCollapse').on('hide.bs.collapse', function (e) {
      if (!jQuery(this).closest('section').hasClass('template')) {
        e.stopPropagation();
      }
    });

    jQuery('.collapse.fkCollapse').on('show.bs.collapse', function (e) {
      if (!jQuery(this).closest('section').hasClass('template')) {
        e.stopPropagation();
      }
    });
    jQuery('.collapse.fkCollapse').on('hide.bs.collapse', function (e) {
      if (!jQuery(this).closest('section').hasClass('template')) {
        e.stopPropagation();
      }
    });

    jQuery('.collapse.indexCollapse').on('show.bs.collapse', function (e) {
      if (!jQuery(this).closest('section').hasClass('template')) {
        e.stopPropagation();
      }
    });
    jQuery('.collapse.indexCollapse').on('hide.bs.collapse', function (e) {
      if (!jQuery(this).closest('section').hasClass('template')) {
        e.stopPropagation();
      }
    });

    jQuery('.collapse.ddlCollapse').on('show.bs.collapse', function () {
      if (!jQuery(this).closest('section').hasClass('template')) {
        jQuery(this).closest('.card').find('.rotate-icon').addClass('down');
        jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
        ddlAccCount = ddlAccCount + 1;
        document.getElementById('ddlExpandButton').innerHTML = 'Collapse All';
      }
    })
    jQuery('.collapse.ddlCollapse').on('hide.bs.collapse', function () {
      if (!jQuery(this).closest('section').hasClass('template')) {
        jQuery(this).closest('.card').find('.rotate-icon').removeClass('down');
        jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
        ddlAccCount = ddlAccCount - 1;
        if (ddlAccCount === 0) {
          document.getElementById('ddlExpandButton').innerHTML = 'Expand All';
        }
      }
    })

    jQuery('.collapse.summaryCollapse').on('show.bs.collapse', function () {
      if (!jQuery(this).closest('section').hasClass('template')) {
        jQuery(this).closest('.card').find('.rotate-icon').addClass('down');
        jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
        summaryAccCount = summaryAccCount + 1;
        document.getElementById('summaryExpandButton').innerHTML = 'Collapse All';
      }
    })
    jQuery('.collapse.summaryCollapse').on('hide.bs.collapse', function () {
      if (!jQuery(this).closest('section').hasClass('template')) {
        jQuery(this).closest('.card').find('.rotate-icon').removeClass('down');
        jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
        summaryAccCount = summaryAccCount - 1;
        if (summaryAccCount === 0) {
          document.getElementById('summaryExpandButton').innerHTML = 'Expand All';
        }
      }
    });
  });
}

export const panelBorderClass = (color) => {
  var borderClass = '';
  switch (color) {
    case 'ORANGE':
      borderClass = ' orangeBorderBottom';
      break;
    case 'GREEN':
      borderClass = ' greenBorderBottom';
      break;
    case 'BLUE':
      borderClass = ' blueBorderBottom';
      break;
    case 'YELLOW':
      borderClass = ' yellowBorderBottom';
      break;
  }
  return borderClass;
}

export const readTextFile = (file, callback) => {
  let rawFile = new XMLHttpRequest();
  rawFile.overrideMimeType("application/json");
  rawFile.open("GET", file, true);
  rawFile.onreadystatechange = function () {
    if (rawFile.status == "404") {
      callback(new Error('File does not exist'), null);
    }
    else if (rawFile.readyState == 4 && rawFile.status == "200") {
      callback(null, rawFile.responseText);
    }
  }
  rawFile.send(null);
}

/**
 * Function to create global edit data type table
 *
 * @return {null}
 */
export const createEditDataTypeTable = () => {
  let globalDataTypeList = JSON.parse(localStorage.getItem('globalDataTypeList'));
  let dataTypeListLength = Object.keys(globalDataTypeList).length;
  for (var i = 0; i < dataTypeListLength; i++) {
    if (document.getElementById('dataTypeRow' + (i + 1)) !== null) {
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
      $dataTypeRow.find('#dataTypeOption' + (i + 1)).unbind('change').bind('change', function () {
        dataTypeUpdate(jQuery(this).attr('id'), globalDataTypeList);
      });
      $dataTypeRow.appendTo(jQuery('#globalDataTypeTable'));
    }
  }
  tooltipHandler();
}