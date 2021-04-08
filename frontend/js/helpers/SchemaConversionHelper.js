

// export const tooltipHandler = () => {
//   jQuery('[data-toggle="tooltip"]').tooltip();
// }

// /**
//  * Function to set style for selected menu
//  *
//  * @param {string} selectedMenuId id of selected menu
//  * @return {null}
//  */
//  const setActiveSelectedMenu = (selectedMenuId) => {
//   jQuery("[name='headerMenu']:not('#"+selectedMenuId+"')").addClass('inactive');
//   jQuery('#'+selectedMenuId).removeClass('inactive');
// }
// export const initSchemaScreenTasks = () => {
//     var reportAccCount = 0;
//     var summaryAccCount = 0;
//     var ddlAccCount = 0;
//     jQuery(document).ready(() => {
//       setActiveSelectedMenu('schemaScreen');
      
//       $(".modal-backdrop").hide();
//       jQuery('.collapse.reportCollapse').on('show.bs.collapse', function () {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           jQuery(this).closest('.card').find('.rotate-icon').addClass('down');
//           jQuery(this).closest('.card').find('.card-header .right-align').toggleClass('show-content hide-content');
//           jQuery(this).closest('.card').find('.report-card-header').toggleClass('borderBottom remBorderBottom');
//           reportAccCount = reportAccCount + 1;
//           document.getElementById('reportExpandButton').innerHTML = 'Collapse All';
//         }
//       });
//       jQuery('.collapse.reportCollapse').on('hide.bs.collapse', function () {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           jQuery(this).closest('.card').find('.rotate-icon').removeClass('down');
//           jQuery(this).closest('.card').find('.card-header .right-align').toggleClass('show-content hide-content');
//           jQuery(this).closest('.card').find('.report-card-header').toggleClass('borderBottom remBorderBottom');
//           reportAccCount = reportAccCount - 1;
//           if (reportAccCount === 0) {
//             document.getElementById('reportExpandButton').innerHTML = 'Expand All';
//           }
//         }
//       });
  
//       jQuery('.collapse.innerSummaryCollapse').on('show.bs.collapse', function (e) {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           e.stopPropagation();
//         }
//       });
//       jQuery('.collapse.innerSummaryCollapse').on('hide.bs.collapse', function (e) {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           e.stopPropagation();
//         }
//       });
  
//       jQuery('.collapse.fkCollapse').on('show.bs.collapse', function (e) {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           e.stopPropagation();
//         }
//       });
//       jQuery('.collapse.fkCollapse').on('hide.bs.collapse', function (e) {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           e.stopPropagation();
//         }
//       });
  
//       jQuery('.collapse.indexCollapse').on('show.bs.collapse', function (e) {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           e.stopPropagation();
//         }
//       });
//       jQuery('.collapse.indexCollapse').on('hide.bs.collapse', function (e) {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           e.stopPropagation();
//         }
//       });
  
//       jQuery('.collapse.ddlCollapse').on('show.bs.collapse', function () {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           jQuery(this).closest('.card').find('.rotate-icon').addClass('down');
//           jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
//           ddlAccCount = ddlAccCount + 1;
//           document.getElementById('ddlExpandButton').innerHTML = 'Collapse All';
//         }
//       })
//       jQuery('.collapse.ddlCollapse').on('hide.bs.collapse', function () {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           jQuery(this).closest('.card').find('.rotate-icon').removeClass('down');
//           jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
//           ddlAccCount = ddlAccCount - 1;
//           if (ddlAccCount === 0) {
//             document.getElementById('ddlExpandButton').innerHTML = 'Expand All';
//           }
//         }
//       })
  
//       jQuery('.collapse.summaryCollapse').on('show.bs.collapse', function () {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           jQuery(this).closest('.card').find('.rotate-icon').addClass('down');
//           jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
//           summaryAccCount = summaryAccCount + 1;
//           document.getElementById('summaryExpandButton').innerHTML = 'Collapse All';
//         }
//       })
//       jQuery('.collapse.summaryCollapse').on('hide.bs.collapse', function () {
//         if (!jQuery(this).closest('section').hasClass('template')) {
//           jQuery(this).closest('.card').find('.rotate-icon').removeClass('down');
//           jQuery(this).closest('.card').find('.ddl-card-header').toggleClass('ddlBorderBottom ddlRemBorderBottom');
//           summaryAccCount = summaryAccCount - 1;
//           if (summaryAccCount === 0) {
//             document.getElementById('summaryExpandButton').innerHTML = 'Expand All';
//           }
//         }
//       });
//     });
//   }

//   export const panelBorderClass = (color) => {
//     var borderClass = '';
//     switch (color) {
//       case 'ORANGE':
//         borderClass = ' orangeBorderBottom';
//         break;
//       case 'GREEN':
//         borderClass = ' greenBorderBottom';
//         break;
//       case 'BLUE':
//         borderClass = ' blueBorderBottom';
//         break;
//       case 'YELLOW':
//         borderClass = ' yellowBorderBottom';
//         break;
//     }
//     return borderClass;
//   }

//   /**
//  * Callback function to read file content
//  *
//  * @param {file}
//  * @return {null}
//  */
// export const readTextFile = (file, callback) => {
//   let rawFile = new XMLHttpRequest();
//   rawFile.overrideMimeType("application/json");
//   rawFile.open("GET", file, true);
//   rawFile.onreadystatechange = function () {
//     if (rawFile.status == "404") {
//       callback(new Error('File does not exist'), null);
//     }
//     else if (rawFile.readyState == 4 && rawFile.status == "200") {
//       callback(null, rawFile.responseText);
//     }
//   }
//   rawFile.send(null);
// }

// /**
//  * Function to create global edit data type table
//  *
//  * @return {null}
//  */
// export const createEditDataTypeTable = () => {
//   let globalDataTypeList = JSON.parse(localStorage.getItem('globalDataTypeList'));
//   let dataTypeListLength = Object.keys(globalDataTypeList).length;
//   for (var i = 0; i < dataTypeListLength; i++) {
//     if (document.getElementById('dataTypeRow' + (i + 1)) !== null) {
//       break
//     }
//     if (globalDataTypeList[Object.keys(globalDataTypeList)[i]] !== null) {
//       let $dataTypeOption;
//       let $dataTypeRow = jQuery('#globalDataTypeTable').find('.globalDataTypeRow.template').clone().removeClass('template');
//       $dataTypeRow.attr('id', 'dataTypeRow' + (i + 1));
//       for (var j = 0; j < 2; j++) {
//         if (j === 0) {
//           $dataTypeRow.find('.src-td').attr('id', 'dataTypeKey' + (i + 1));
//           $dataTypeRow.find('.src-td').html(Object.keys(globalDataTypeList)[i]);
//         }
//         else if (j === 1) {
//           $dataTypeRow.find('#globalDataTypeCell').attr('id', 'dataTypeVal' + (i + 1));
//           let optionsLength = globalDataTypeList[Object.keys(globalDataTypeList)[i]].length;
//           if (globalDataTypeList[Object.keys(globalDataTypeList)[i]][0].Brief !== "") {
//             $dataTypeRow.find('i').attr('data-toggle', 'tooltip');
//             $dataTypeRow.find('i').attr('data-placement', 'bottom');
//             $dataTypeRow.find('i').attr('title', globalDataTypeList[Object.keys(globalDataTypeList)[i]][0].Brief);
//           }
//           else {
//             $dataTypeRow.find('i').css('visibility', 'hidden');
//           }
//           $dataTypeRow.find('select').attr('id', 'dataTypeOption' + (i + 1));
//           for (var k = 0; k < optionsLength; k++) {
//             $dataTypeOption = $dataTypeRow.find('.dataTypeOption.template').clone().removeClass('template');
//             $dataTypeOption.attr('value', globalDataTypeList[Object.keys(globalDataTypeList)[i]][k].T);
//             $dataTypeOption.html(globalDataTypeList[Object.keys(globalDataTypeList)[i]][k].T);
//             $dataTypeOption.appendTo($dataTypeRow.find('select'));
//           }
//         }
//       }
//       $dataTypeRow.find('select').find("option").eq(0).remove();
//       $dataTypeRow.find('#dataTypeOption' + (i + 1)).unbind('change').bind('change', function () {
//         dataTypeUpdate(jQuery(this).attr('id'), globalDataTypeList);
//       });
//       $dataTypeRow.appendTo(jQuery('#globalDataTypeTable'));
//     }
//   }
//   tooltipHandler();
// }

// export const showSnackbar = (message, bgClass) => {
//   var snackbar = document.getElementById("snackbar");
//   snackbar.classList.add("show",bgClass);
//   snackbar.innerHTML = message;
//   setTimeout(function () {
//     snackbar.classList.remove("show",bgClass);
//   }, 3000);
// }

// export const tabbingHelper = (id,others) => {

//     document.getElementById(id+"SearchForm").style.display = "inline-block";
//     document.getElementById(id+"Tab").classList.add("active", "show");
//     document.getElementById(id).classList.add("active", "show");

//     others.map((element)=>{
//     document.getElementById(element+"SearchForm").style.setProperty("display", "none", "important");
//     document.getElementById(element+"Tab").classList.remove("active", "show");
//     document.getElementById(element).classList.remove("active", "show");
//     })
    
// }

// export const dataTypeUpdate = (id, globalDataTypeList) => {
//   let idNum = parseInt(id.match(/\d+/), 10);
//   let dataTypeOptionArray = globalDataTypeList[document.getElementById('dataTypeKey' + idNum).innerHTML];
//   let optionFound;
//   let length = dataTypeOptionArray.length;
//   let $dataTypeSel = jQuery('.globalDataTypeRow.template').clone();
//   $dataTypeSel.find('.src-td').attr('id', 'dataTypeKey' + idNum);
//   $dataTypeSel.find('.src-td').html(Object.keys(globalDataTypeList)[idNum - 1]);
//   $dataTypeSel.find('i').css('visibility', 'hidden');
//   for (var x = 0; x < length; x++) {
//     let $dataTypeOption = $dataTypeSel.find('.dataTypeOption.template').clone().removeClass('template');
//     optionFound = dataTypeOptionArray[x].T === document.getElementById(id).value;
//     if (dataTypeOptionArray[x].T === document.getElementById(id).value && dataTypeOptionArray[x].Brief !== "") {
//       $dataTypeSel.find('i').attr('data-toggle', 'tooltip');
//       $dataTypeSel.find('i').attr('data-placement', 'bottom');
//       $dataTypeSel.find('i').attr('title', dataTypeOptionArray[x].Brief);
//       $dataTypeSel.find('i').css('visibility', '');
//     }
//     if (optionFound === true) {
//       $dataTypeOption.attr('value', dataTypeOptionArray[x].T);
//       $dataTypeOption.html(dataTypeOptionArray[x].T);
//       $dataTypeOption.attr('selected', 'selected');
//     }
//     else {
//       $dataTypeOption.attr('value', dataTypeOptionArray[x].T);
//       $dataTypeOption.html(dataTypeOptionArray[x].T);
//     }
//     $dataTypeOption.appendTo($dataTypeSel.find('select'));
//   }
//   $dataTypeSel.find('select').find("option").eq(0).remove();
//   $dataTypeSel.find('select').attr('id', id);
//   jQuery(this).unbind('change').bind('change', function () {
//     dataTypeUpdate(id, globalDataTypeList);
//   });
//   jQuery("#dataTypeRow" + idNum).html($dataTypeSel.html());
//   tooltipHandler();
// }





export const tooltipHandler = () => {
  jQuery('[data-toggle="tooltip"]').tooltip();
}

/**
 * Function to set style for selected menu
 *
 * @param {string} selectedMenuId id of selected menu
 * @return {null}
 */
 const setActiveSelectedMenu = (selectedMenuId) => {
  jQuery("[name='headerMenu']:not('#"+selectedMenuId+"')").addClass('inactive');
  jQuery('#'+selectedMenuId).removeClass('inactive');
}
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

export const mdcCardBorder = (color) => {
  var cardBorderClass = '';
  switch (color) {
    case 'ORANGE':
      cardBorderClass = ' cardOrangeBorder';
      break;
    case 'GREEN':
      cardBorderClass = ' cardGreenBorder';
      break;
    case 'BLUE':
      cardBorderClass = ' cardBlueBorder';
      break;
    case 'YELLOW':
      cardBorderClass = ' cardYellowBorder';
  }
  return cardBorderClass;
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

export const showSnackbar = (message, bgClass) => {
  var snackbar = document.getElementById("snackbar");
  snackbar.className = "show" + bgClass;
  snackbar.innerHTML = message;
  setTimeout(function () {
    snackbar.className = snackbar.className.replace("show", "");
  }, 3000);
}

export const tabbingHelper = (id,others) => {

    document.getElementById(id+"SearchForm").style.display = "inline-block";
    document.getElementById(id+"Tab").classList.add("active", "show");
    document.getElementById(id).classList.add("active", "show");

    others.map((element)=>{
    document.getElementById(element+"SearchForm").style.setProperty("display", "none", "important");
    document.getElementById(element+"Tab").classList.remove("active", "show");
    document.getElementById(element).classList.remove("active", "show");
    })
    
}

export const dataTypeUpdate = (id, globalDataTypeList) => {
  let idNum = parseInt(id.match(/\d+/), 10);
  let dataTypeOptionArray = globalDataTypeList[document.getElementById('dataTypeKey' + idNum).innerHTML];
  let optionFound;
  let length = dataTypeOptionArray.length;
  let $dataTypeSel = jQuery('.globalDataTypeRow.template').clone();
  $dataTypeSel.find('.src-td').attr('id', 'dataTypeKey' + idNum);
  $dataTypeSel.find('.src-td').html(Object.keys(globalDataTypeList)[idNum - 1]);
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
  jQuery(this).unbind('change').bind('change', function () {
    dataTypeUpdate(id, globalDataTypeList);
  });
  jQuery("#dataTypeRow" + idNum).html($dataTypeSel.html());
  tooltipHandler();
}

