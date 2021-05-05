export const tooltipHandler = () => {
  jQuery('[data-toggle="tooltip"]').tooltip();
};

export const setActiveSelectedMenu = (selectedMenuId) => {
  jQuery("[name='headerMenu']:not('#" + selectedMenuId + "')").addClass(
    "inactive"
  );
  jQuery("#" + selectedMenuId).removeClass("inactive");
};

export const initSchemaScreenTasks = () => {
  var reportAccCount = 0;
  var summaryAccCount = 0;
  var ddlAccCount = 0;

  jQuery(document).ready(() => {
    setActiveSelectedMenu("schemaScreen");
    $(".modal-backdrop").hide();

    jQuery(".collapse.inner-summary-collapse").on(
      "show.bs.collapse",
      function (e) {
        if (!jQuery(this).closest("section").hasClass("template")) {
          e.stopPropagation();
        }
      }
    );
    jQuery(".collapse.inner-summary-collapse").on(
      "hide.bs.collapse",
      function (e) {
        if (!jQuery(this).closest("section").hasClass("template")) {
          e.stopPropagation();
        }
      }
    );

    jQuery(".collapse.fk-collapse").on("show.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });
    jQuery(".collapse.fk-collapse").on("hide.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });

    jQuery(".collapse.index-collapse").on("show.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });
    jQuery(".collapse.index-collapse").on("hide.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });
  });
};

export const panelBorderClass = (color) => {
  var borderClass = "";
  switch (color) {
    case "ORANGE":
      borderClass = " orange-border-bottom";
      break;
    case "GREEN":
      borderClass = " green-border-bottom";
      break;
    case "BLUE":
      borderClass = " blue-border-bottom";
      break;
    case "YELLOW":
      borderClass = " yellow-border-bottom";
      break;
  }
  return borderClass;
};

export const mdcCardBorder = (color) => {
  var cardBorderClass = "";
  switch (color) {
    case "ORANGE":
      cardBorderClass = " card-orange-border";
      break;
    case "GREEN":
      cardBorderClass = " card-green-border";
      break;
    case "BLUE":
      cardBorderClass = " card-blue-border";
      break;
    case "YELLOW":
      cardBorderClass = " card-yellow-border";
  }
  return cardBorderClass;
};

export const readTextFile = (file, callback) => {
  let rawFile = new XMLHttpRequest();
  rawFile.overrideMimeType("application/json");
  rawFile.open("GET", file, true);
  rawFile.onreadystatechange = function () {
    if (rawFile.readyState == 4 && rawFile.status == "404") {
      callback(new Error("File does not exist"), null);
    } else if (rawFile.readyState == 4 && rawFile.status == "200") {
      callback(null, rawFile.responseText);
    }
  };
  rawFile.send(null);
};

export const showSnackbar = (message, bgClass) => {
  var snackbar = document.getElementById("snackbar");
  snackbar.className = "show-snackbar" + bgClass;
  snackbar.innerHTML = message;
  setTimeout(function () {
    snackbar.className = snackbar.className.replace("show-snackbar", "");
  }, 3000);
};

export const recreateNode = (el) => {
  let newEl = el.cloneNode(false);
  el.parentNode.replaceChild(newEl, el);
};

export const checkBoxStateHandler = (tableIndex,numOfColumn)=> {
  let uncheckCount;
  let checkAllTableNumber = jQuery("#chck-all-" + tableIndex);
  let checkClassTableNumber = jQuery(".chck-class-" + tableIndex);

  checkAllTableNumber.click(function () {
    checkClassTableNumber = jQuery(".chck-class-" + tableIndex);
    switch (jQuery(this).is(":checked")) {
      case true:
        checkClassTableNumber.prop("checked", true);
        uncheckCount = 0;
        break;
      case false:
        checkClassTableNumber.prop("checked", false);
        uncheckCount = numOfColumn;
        break;
    }
  });

  checkClassTableNumber.click(function () {
    checkAllTableNumber = jQuery("#chck-all-" + tableIndex);
    if (jQuery(this).is(":checked")) {
      uncheckCount = uncheckCount - 1;
      if (uncheckCount === 0) {
        checkAllTableNumber.prop("checked", true);
      }
    } else {
      uncheckCount = uncheckCount + 1;
      checkAllTableNumber.prop("checked", false);
    }
  });
}

export const editButtonHandler = (tableNumber, notNullConstraint) => {
  let tableId = '#src-sp-table' + tableNumber + ' tr';
  let tableColumnNumber = 0;
  jQuery(tableId).each(function (index) {
    if (index > 1) {
      let constraintId = 'sp-constraint-' + tableNumber + tableColumnNumber;
      jQuery('#sp-constraint-' + tableNumber + tableColumnNumber).on('change', function () {
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
      tableColumnNumber++;
    }
  });
}