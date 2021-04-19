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
    jQuery(".collapse.reportCollapse").on("show.bs.collapse", function () {
      if (!jQuery(this).closest("section").hasClass("template")) {
        jQuery(this).closest(".card").find(".rotate-icon").addClass("down");
        jQuery(this)
          .closest(".card")
          .find(".card-header .right-align")
          .toggleClass("show-content hide-content");
        jQuery(this)
          .closest(".card")
          .find(".report-card-header")
          .toggleClass("borderBottom remBorderBottom");
        reportAccCount = reportAccCount + 1;
        document.getElementById("reportExpandButton").innerHTML =
          "Collapse All";
      }
    });
    jQuery(".collapse.reportCollapse").on("hide.bs.collapse", function () {
      if (!jQuery(this).closest("section").hasClass("template")) {
        jQuery(this).closest(".card").find(".rotate-icon").removeClass("down");
        jQuery(this)
          .closest(".card")
          .find(".card-header .right-align")
          .toggleClass("show-content hide-content");
        jQuery(this)
          .closest(".card")
          .find(".report-card-header")
          .toggleClass("borderBottom remBorderBottom");
        reportAccCount = reportAccCount - 1;
        if (reportAccCount === 0) {
          document.getElementById("reportExpandButton").innerHTML =
            "Expand All";
        }
      }
    });

    jQuery(".collapse.innerSummaryCollapse").on(
      "show.bs.collapse",
      function (e) {
        if (!jQuery(this).closest("section").hasClass("template")) {
          e.stopPropagation();
        }
      }
    );
    jQuery(".collapse.innerSummaryCollapse").on(
      "hide.bs.collapse",
      function (e) {
        if (!jQuery(this).closest("section").hasClass("template")) {
          e.stopPropagation();
        }
      }
    );

    jQuery(".collapse.fkCollapse").on("show.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });
    jQuery(".collapse.fkCollapse").on("hide.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });

    jQuery(".collapse.indexCollapse").on("show.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });
    jQuery(".collapse.indexCollapse").on("hide.bs.collapse", function (e) {
      if (!jQuery(this).closest("section").hasClass("template")) {
        e.stopPropagation();
      }
    });

    jQuery(".collapse.ddlCollapse").on("show.bs.collapse", function () {
      if (!jQuery(this).closest("section").hasClass("template")) {
        jQuery(this).closest(".card").find(".rotate-icon").addClass("down");
        jQuery(this)
          .closest(".card")
          .find(".ddl-card-header")
          .toggleClass("ddlBorderBottom ddlRemBorderBottom");
        ddlAccCount = ddlAccCount + 1;
        document.getElementById("ddlExpandButton").innerHTML = "Collapse All";
      }
    });
    jQuery(".collapse.ddlCollapse").on("hide.bs.collapse", function () {
      if (!jQuery(this).closest("section").hasClass("template")) {
        jQuery(this).closest(".card").find(".rotate-icon").removeClass("down");
        jQuery(this)
          .closest(".card")
          .find(".ddl-card-header")
          .toggleClass("ddlBorderBottom ddlRemBorderBottom");
        ddlAccCount = ddlAccCount - 1;
        if (ddlAccCount === 0) {
          document.getElementById("ddlExpandButton").innerHTML = "Expand All";
        }
      }
    });

    jQuery(".collapse.summaryCollapse").on("show.bs.collapse", function () {
      if (!jQuery(this).closest("section").hasClass("template")) {
        jQuery(this).closest(".card").find(".rotate-icon").addClass("down");
        jQuery(this)
          .closest(".card")
          .find(".ddl-card-header")
          .toggleClass("ddlBorderBottom ddlRemBorderBottom");
        summaryAccCount = summaryAccCount + 1;
        document.getElementById("summaryExpandButton").innerHTML =
          "Collapse All";
      }
    });
    jQuery(".collapse.summaryCollapse").on("hide.bs.collapse", function () {
      if (!jQuery(this).closest("section").hasClass("template")) {
        jQuery(this).closest(".card").find(".rotate-icon").removeClass("down");
        jQuery(this)
          .closest(".card")
          .find(".ddl-card-header")
          .toggleClass("ddlBorderBottom ddlRemBorderBottom");
        summaryAccCount = summaryAccCount - 1;
        if (summaryAccCount === 0) {
          document.getElementById("summaryExpandButton").innerHTML =
            "Expand All";
        }
      }
    });
  });
};

export const panelBorderClass = (color) => {
  var borderClass = "";
  switch (color) {
    case "ORANGE":
      borderClass = " orangeBorderBottom";
      break;
    case "GREEN":
      borderClass = " greenBorderBottom";
      break;
    case "BLUE":
      borderClass = " blueBorderBottom";
      break;
    case "YELLOW":
      borderClass = " yellowBorderBottom";
      break;
  }
  return borderClass;
};

export const mdcCardBorder = (color) => {
  var cardBorderClass = "";
  switch (color) {
    case "ORANGE":
      cardBorderClass = " cardOrangeBorder";
      break;
    case "GREEN":
      cardBorderClass = " cardGreenBorder";
      break;
    case "BLUE":
      cardBorderClass = " cardBlueBorder";
      break;
    case "YELLOW":
      cardBorderClass = " cardYellowBorder";
  }
  return cardBorderClass;
};

export const readTextFile = (file, callback) => {
  let rawFile = new XMLHttpRequest();
  rawFile.overrideMimeType("application/json");
  rawFile.open("GET", file, true);
  rawFile.onreadystatechange = function () {
    if (rawFile.status == "404") {
      callback(new Error("File does not exist"), null);
    } else if (rawFile.readyState == 4 && rawFile.status == "200") {
      callback(null, rawFile.responseText);
    }
  };
  rawFile.send(null);
};

export const showSnackbar = (message, bgClass) => {
  var snackbar = document.getElementById("snackbar");
  snackbar.className = "show" + bgClass;
  snackbar.innerHTML = message;
  setTimeout(function () {
    snackbar.className = snackbar.className.replace("show", "");
  }, 3000);
};

export const recreateNode = (el) => {
  let newEl = el.cloneNode(false);
  el.parentNode.replaceChild(newEl, el);
};