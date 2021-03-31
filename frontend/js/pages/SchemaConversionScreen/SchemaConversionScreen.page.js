import "./../../components/Tab/Tab.component.js";
import "./../../components/TableCarousel/TableCarousel.component.js";
import {initSchemaScreenTasks} from "./../../helpers/SchemaConversionHelper.js";

// Services
import Store from "./../../services/Store.service.js";

const TAB_CONFIG_DATA = [
  {
    id: "reportTab",
    text: "Conversion Report",
  },
  {
    id: "ddlTab",
    text: "DDL Statements",
  },
  {
    id: "summaryTab",
    text: "Summary Report",
  },
];

class SchemaConversionScreen extends HTMLElement {
  connectedCallback() {
    this.stateObserver = setInterval(this.observeState, 200);
    this.render();
    // this.createSourceAndSpannerTables();
  }

  disconnectedCallback() {
    clearInterval(this.stateObserver);
  }

  observeState = () => {
    if (JSON.stringify(Store.getinstance()) !== JSON.stringify(this.data)) {
      this.data = Store.getinstance();
      this.render();
    }
  };

  getGlobalDataTypeList = () => {
    fetch("/typemap", {
      method: "GET",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
    })
      .then(function (res) {
        if (res.ok) {
          res.json().then(function (result) {
            localStorage.setItem("globalDataTypeList", JSON.stringify(result));
          });
        } else {
          return Promise.reject(res);
        }
      })
      .catch(function (err) {
        showSnackbar(err, " redBg");
      });
  };

  createDdlFromJson = (result) => {
    let ddl = result;
    let ddlLength = Object.keys(ddl).length;
    let createIndex, createEndIndex, $newDdlElement;
    let conversionRateResp = {};
    conversionRateResp = JSON.parse(localStorage.getItem("tableBorderColor"));
    for (var i = 0; i < ddlLength; i++) {
      createIndex = ddl[Object.keys(ddl)[i]].search("CREATE TABLE");
      createEndIndex = createIndex + 12;
      ddl[Object.keys(ddl)[i]] =
        ddl[Object.keys(ddl)[i]].substring(0, createIndex) +
        ddl[Object.keys(ddl)[i]]
          .substring(createIndex, createEndIndex)
          .fontcolor("#4285f4")
          .bold() +
        ddl[Object.keys(ddl)[i]].substring(createEndIndex);
      $newDdlElement = jQuery("#ddlDiv")
        .find(".ddlSection.template")
        .clone()
        .removeClass("template");
      $newDdlElement.attr("id", "ddl" + i);
      $newDdlElement
        .find(".card-header.ddl-card-header.ddlBorderBottom")
        .addClass(panelBorderClass(conversionRateResp[srcTableName[i]]));
      $newDdlElement.find("a").attr("href", "#" + Object.keys(ddl)[i] + "-ddl");
      $newDdlElement.find("a > span").html(Object.keys(ddl)[i]);
      $newDdlElement
        .find(".collapse.ddlCollapse")
        .attr("id", Object.keys(ddl)[i] + "-ddl");
      $newDdlElement
        .find(".mdc-card.mdc-card-content.ddl-border.table-card-border")
        .addClass(mdcCardBorder(conversionRateResp[srcTableName[i]]));
      $newDdlElement
        .find("code")
        .html(
          ddl[srcTableName[i]].split("\n").join(`<span class='sql-c'></span>`)
        );
      $newDdlElement.appendTo("#ddlDiv");
    }
  };

  createSummaryFromJson = (result) => {
    let summary = result;
    let summaryLength = Object.keys(summary).length;
    let summaryContent = "";
    let $newSummaryElement;
    let conversionRateResp = {};
    conversionRateResp = JSON.parse(localStorage.getItem("tableBorderColor"));
    for (var i = 0; i < summaryLength; i++) {
      $newSummaryElement = jQuery("#summaryDiv")
        .find(".summarySection.template")
        .clone()
        .removeClass("template");
      $newSummaryElement.attr("id", "summary" + i);
      $newSummaryElement
        .find(".card-header.ddl-card-header.ddlBorderBottom")
        .addClass(panelBorderClass(conversionRateResp[srcTableName[i]]));
      $newSummaryElement
        .find("a")
        .attr("href", "#" + Object.keys(summary)[i] + "-summary");
      $newSummaryElement.find("a > span").html(Object.keys(summary)[i]);
      $newSummaryElement
        .find(".collapse.summaryCollapse")
        .attr("id", Object.keys(summary)[i] + "-summary");
      $newSummaryElement
        .find(".mdc-card.mdc-card-content.ddl-border.table-card-border")
        .addClass(mdcCardBorder(conversionRateResp[srcTableName[i]]));
      $newSummaryElement
        .find(".mdc-card.summary-content")
        .html(summary[srcTableName[i]].split("\n").join("<br />"));
      $newSummaryElement.appendTo("#summaryDiv");
    }
  };

  createSourceAndSpannerTables = async () => {
    // hideSpinner();
    schemaConversionObj = JSON.parse(localStorage.getItem("conversionReportContent"));
    console.log(schemaConversionObj);
    this.getGlobalDataTypeList();
    // schemaConversionObj = obj;
    let columnNameContent,
      dataTypeContent,
      constraintsContent,
      notNullFound,
      constraintId,
      srcConstraintHtml, pkFlag, keyIconValue, keyColumnObj;
    let pksSp = [],
      initialColNameArray = [],
      notNullFoundFlag = [],
      pkSeqId = [],
      initialPkSeqId = [],
      constraintTabCell = [],
      primaryTabCell = [],
      spPlaceholder = [],
      srcPlaceholder = [],
      countSp = [],
      countSrc = [];
    let sourceTableFlag = "";
    let conversionRateResp = {};
    let constraintCount = 0;
    let srcTableNum = Object.keys(schemaConversionObj.SrcSchema).length;
    let spTable_num = Object.keys(schemaConversionObj.SpSchema).length;
    let srcTable,
      spTable,
      spTableCols,
      pkArrayLength,
      columnsLength,
      currentColumnSp,
      currentColumnSrc,
      pksSpLength,
      $newConvElement,
      $convTableContent,
      $fkTableContent,
      $indexTableContent;

    for (var x = 0; x < srcTableNum; x++) {
      initialPkSeqId[x] = [];
      constraintTabCell[x] = [];
      primaryTabCell[x] = [];
      notPrimary[x] = [];
      notNullFoundFlag[x] = [];
      pkArray[x] = [];
      srcPlaceholder[x] = [];
      spPlaceholder[x] = [];
      countSp[x] = [];
      countSrc[x] = [];
      pksSp[x] = [];
    }

    conversionRateResp = JSON.parse(localStorage.getItem("tableBorderColor"));
    for (var i = 0; i < srcTableNum; i++) {
      debugger
      srcTable =
        schemaConversionObj.SrcSchema[
        Object.keys(schemaConversionObj.ToSpanner)[i]
        ];
      srcTableName[i] = Object.keys(schemaConversionObj.ToSpanner)[i];
      spTable = schemaConversionObj.SpSchema[srcTableName[i]];
      spTableCols = spTable.ColNames;
      pkArray[i] =
        schemaConversionObj.SpSchema[
          Object.keys(schemaConversionObj.SpSchema)[i]
        ].Pks;
      pkSeqId[i] = 1;
      pkArrayLength = pkArray[i].length;
      if (pkArrayLength === 1 && pkArray[i][0].Col === "synth_id")
        pkArrayLength = 0;
      columnsLength = Object.keys(
        schemaConversionObj.ToSpanner[spTable.Name].Cols
      ).length;
      for (var x = 0; x < pkArrayLength; x++) {
        if (pkArray[i][x].seqId == undefined) {
          pkArray[i][x].seqId = pkSeqId[i];
          pkSeqId[i]++;
        }
      }
      schemaConversionObj.SpSchema[srcTableName[i]].Pks = pkArray[i];
      sourceTableFlag = localStorage.getItem("sourceDbName");
      $newConvElement = jQuery("#reportDiv")
        .find(".reportSection.template")
        .clone()
        .removeClass("template");
      $newConvElement.attr("id", i);
      $newConvElement
        .find(".card-header.report-card-header.borderBottom")
        .addClass(panelBorderClass(conversionRateResp[srcTableName[i]]));
      $newConvElement
        .find("a")
        .attr("href", "#" + Object.keys(schemaConversionObj.SrcSchema)[i]);
      $newConvElement
        .find("a > span")
        .html(Object.keys(schemaConversionObj.SrcSchema)[i]);
      $newConvElement
        .find(".right-align.edit-button.hide-content")
        .attr("id", "editSpanner" + i);
      $newConvElement
        .find(".right-align.editInstruction.hide-content")
        .attr("id", "editInstruction" + i);
      $newConvElement.find("#editSpanner" + i).click(function () {
        let index = parseInt(jQuery(this).attr("id").match(/\d+/), 10);
        schemaConversionObj = JSON.parse(
          localStorage.getItem("conversionReportContent")
        );
        let spTable = schemaConversionObj.SpSchema[srcTableName[index]];
        initialColNameArray[index] = [];
        if (jQuery(this).html().trim() === "Edit Spanner Schema") {
          if (spTable.Fks != null && spTable.Fks.length != 0) {
            jQuery("#saveInterleave" + index).removeAttr("disabled");
            jQuery("#add" + index).removeAttr("disabled");
            jQuery("#interleave" + index).removeAttr("disabled");
            for (var p = 0; p < spTable.Fks.length; p++) {
              jQuery("#" + srcTableName[index] + p + "foreignKey").removeAttr(
                "disabled"
              );
            }
          }
          if (spTable.Indexes != null && spTable.Indexes.length != 0) {
            for (var p = 0; p < spTable.Indexes.length; p++) {
              jQuery("#" + srcTableName[index] + p + "secIndex").removeAttr(
                "disabled"
              );
            }
          }
        } else {
          if (spTable.Fks != null && spTable.Fks.length != 0) {
            jQuery("#saveInterleave" + index).attr("disabled", "disabled");
            jQuery("#add" + index).attr("disabled", "disabled");
            jQuery("#interleave" + index).attr("disabled", "disabled");
            for (var p = 0; p < spTable.Fks.length; p++) {
              jQuery("#" + srcTableName[index] + p + "foreignKey").attr(
                "disabled",
                "disabled"
              );
            }
          }
          if (spTable.Indexes != null && spTable.Indexes.length != 0) {
            for (var p = 0; p < spTable.Indexes.length; p++) {
              jQuery("#" + srcTableName[index] + p + "secIndex").attr(
                "disabled",
                "disabled"
              );
            }
          }
        }
        editAndSaveButtonHandler(
          jQuery(this),
          spPlaceholder[index],
          pkArray[index],
          notNullFoundFlag[index],
          initialColNameArray[index],
          notPrimary[index]
        );
      });
      $newConvElement
        .find(".collapse.reportCollapse")
        .attr("id", Object.keys(schemaConversionObj.SrcSchema)[i]);
      $newConvElement
        .find(".mdc-card.mdc-card-content.table-card-border")
        .addClass(mdcCardBorder(conversionRateResp[srcTableName[i]]));
      $newConvElement.find(".acc-table").attr("id", "src-sp-table" + i);
      $newConvElement.find(".acc-table-th-src").append(sourceTableFlag);

      for (var k = 0; k < columnsLength; k++) {
        $convTableContent = $newConvElement
          .find(".reportTableContent.template")
          .clone()
          .removeClass("template");
        currentColumnSrc = Object.keys(
          schemaConversionObj.ToSpanner[spTable.Name].Cols
        )[k];
        currentColumnSp =
          schemaConversionObj.ToSpanner[spTable.Name].Cols[currentColumnSrc];
        pksSp[i] = [...spTable.Pks];
        pksSpLength = pksSp[i].length;
        $convTableContent
          .find(".saveColumnName.template")
          .removeClass("template")
          .attr("id", "saveColumnName" + i + k);
        $convTableContent
          .find(".editColumnName.template")
          .attr("id", "editColumnName" + i + k);
        $convTableContent
          .find(".editDataType.template")
          .attr("id", "editDataType" + i + k);
        $convTableContent
          .find(".saveConstraint.template")
          .removeClass("template")
          .attr("id", "saveConstraint" + i + k);
        $convTableContent
          .find(".editConstraint.template")
          .attr("id", "editConstraint" + i + k);
        if (
          srcTable.PrimaryKeys === null ||
          srcTable.PrimaryKeys[0].Column !== currentColumnSrc
        ) {
          $convTableContent.find(".srcPk").css("visibility", "hidden");
        }
        $convTableContent
          .find(".column.right.srcColumn")
          .html(currentColumnSrc);
        $convTableContent
          .find(".column.right.srcColumn")
          .attr("id", "srcColumn" + k);

        $convTableContent
          .find(".sp-column.acc-table-td.spannerColName")
          .addClass("spannerTabCell" + i + k);
        pkFlag = false;
        for (var x = 0; x < pksSpLength; x++) {
          if (pksSp[i][x].Col === currentColumnSp) {
            pkFlag = true;
            $convTableContent
              .find(".column.left.spannerPkSpan")
              .attr("data-toggle", "tooltip");
            $convTableContent
              .find(".column.left.spannerPkSpan")
              .attr("data-placement", "bottom");
            $convTableContent
              .find(".column.left.spannerPkSpan")
              .attr("title", "primary key: " + currentColumnSp);
            $convTableContent
              .find(".column.left.spannerPkSpan")
              .attr("id", "keyIcon" + i + k + k);
            $convTableContent
              .find(".column.left.spannerPkSpan")
              .css("cursor", "pointer");
            $convTableContent
              .find(".column.left.spannerPkSpan > sub")
              .html(pksSp[i][x].seqId);

            $convTableContent
              .find(".column.right.spannerColNameSpan")
              .attr("data-toggle", "tooltip");
            $convTableContent
              .find(".column.right.spannerColNameSpan")
              .attr("data-placement", "bottom");
            $convTableContent
              .find(".column.right.spannerColNameSpan")
              .attr("title", "primary key: " + currentColumnSp);
            $convTableContent
              .find(".column.right.spannerColNameSpan")
              .attr("id", "columnNameText" + i + k + k);
            $convTableContent
              .find(".column.right.spannerColNameSpan")
              .css("cursor", "pointer");
            $convTableContent
              .find(".column.right.spannerColNameSpan")
              .html(currentColumnSp);
            notPrimary[i][k] = false;
            initialPkSeqId[i][k] = pksSp[i][x].seqId;
            break;
          }
        }
        if (pkFlag === false) {
          notPrimary[i][k] = true;
          $convTableContent
            .find(".column.left.spannerPkSpan")
            .attr("id", "keyIcon" + i + k + k);
          $convTableContent
            .find($convTableContent.find(".column.left.spannerPkSpan > img"))
            .css("visibility", "hidden");
          $convTableContent
            .find(".column.right.spannerColNameSpan")
            .attr("id", "columnNameText" + i + k + k);
          $convTableContent
            .find(".column.right.spannerColNameSpan")
            .html(currentColumnSp);
        }
        primaryTabCell[i][k] = $convTableContent;
        keyIconValue = "keyIcon" + i + k + k;
        keyColumnObj = { keyIconId: keyIconValue, columnName: currentColumnSp };

        $convTableContent
          .find(".acc-table-td.srcDataType")
          .attr("id", "srcDataType" + i + k);
        $convTableContent
          .find(".acc-table-td.srcDataType")
          .html(srcTable.ColDefs[currentColumnSrc].Type.Name);
        $convTableContent
          .find(".sp-column.acc-table-td.spannerDataType")
          .attr("id", "dataType" + i + k);
        $convTableContent
          .find(".saveDataType.template")
          .removeClass("template")
          .attr("id", "saveDataType" + i + k)
          .html(spTable.ColDefs[currentColumnSp].T.Name);
        $convTableContent
          .find(".sp-column.acc-table-td.spannerDataType")
          .addClass("spannerTabCell" + i + k);

        countSrc[i][k] = 0;
        srcPlaceholder[i][k] = countSrc[i][k];
        if (srcTable.ColDefs[currentColumnSrc].NotNull !== undefined) {
          if (srcTable.ColDefs[currentColumnSrc].NotNull === true) {
            countSrc[i][k] = countSrc[i][k] + 1;
            srcPlaceholder[i][k] = countSrc[i][k];
            $convTableContent.find(".srcNotNullConstraint").addClass("active");
          }
        }
        constraintId = "srcConstraint" + i + k;
        $convTableContent
          .find(".form-control.spanner-input.tableSelect.srcConstraint")
          .attr("id", constraintId);

        countSp[i][k] = 0;
        spPlaceholder[i][k] = countSp[i][k];
        $convTableContent
          .find(".acc-table-td.sp-column.acc-table-td")
          .addClass("spannerTabCell" + i + k);
        // checking not null consraint
        if (spTable.ColDefs[currentColumnSp].NotNull !== undefined) {
          if (spTable.ColDefs[currentColumnSp].NotNull === true) {
            countSp[i][k] = countSp[i][k] + 1;
            spPlaceholder[i][k] = countSp[i][k];
            $convTableContent
              .find(".spannerNotNullConstraint")
              .addClass("active");
            notNullFoundFlag[i][k] = true;
            notNullConstraint[parseInt(String(i) + String(k))] = "Not Null";
          } else {
            notNullFoundFlag[i][k] = false;
            notNullConstraint[parseInt(String(i) + String(k))] = "";
          }
        }
        constraintId = "spConstraint" + i + k;
        $convTableContent
          .find(".form-control.spanner-input.tableSelect.spannerConstraint")
          .attr("id", constraintId);
        constraintTabCell[i][k] = $convTableContent;
        $convTableContent.appendTo($newConvElement.find(".acc-table-body"));
      }
      $newConvElement.find(".acc-table-body").find("tr").eq(0).remove();
      if (spTable.Fks != null && spTable.Fks.length != 0) {
        let foreignKeyId, tableNumber;
        $newConvElement.find(".fkCard").removeClass("template");
        $newConvElement.find(".fkFont").attr("href", "#foreignKey" + i);
        $newConvElement.find("fieldset").attr("id", "radioBtnArea" + i);
        $newConvElement.find(".fkFont").html("Foreign Keys");
        $newConvElement
          .find(".collapse.fkCollapse")
          .attr("id", "foreignKey" + i);
        $newConvElement.find(".radio.addRadio").attr("id", "add" + i);
        $newConvElement.find("#add" + i).attr("name", "fks" + i);
        $newConvElement
          .find(".radio.interleaveRadio")
          .attr("id", "interleave" + i);
        $newConvElement.find("#interleave" + i).attr("name", "fks" + i);
        // checkInterleaveConversion(i);
        $newConvElement.find(".fkTableBody").attr("id", "fkTableBody" + i);
        for (var p = 0; p < spTable.Fks.length; p++) {
          $fkTableContent = $newConvElement
            .find(".fkTableTr.template")
            .clone()
            .removeClass("template");
          $fkTableContent
            .find(".renameFk.template")
            .attr("id", "renameFk" + i + p);
          $fkTableContent
            .find(".saveFk.template")
            .removeClass("template")
            .attr("id", "saveFk" + i + p)
            .html(spTable.Fks[p].Name);
          $fkTableContent
            .find(".acc-table-td.fkTableColumns")
            .html(spTable.Fks[p].Columns);
          $fkTableContent
            .find(".acc-table-td.fkTableReferTable")
            .html(spTable.Fks[p].ReferTable);
          $fkTableContent
            .find(".acc-table-td.fkTableReferColumns")
            .html(spTable.Fks[p].ReferColumns);
          $fkTableContent
            .find("button")
            .attr("id", spTable.Name + p + "foreignKey");
          $fkTableContent
            .find("#" + spTable.Name + p + "foreignKey")
            .click(function () {
              tableNumber = parseInt(
                jQuery(this)
                  .closest(".collapse.fkCollapse")
                  .attr("id")
                  .match(/\d+/),
                10
              );
              foreignKeyId = jQuery(this).attr("id");
              localStorage.setItem("foreignKeyId", foreignKeyId);
              localStorage.setItem("tableNumber", tableNumber);
              jQuery("#foreignKeyDeleteWarning").modal();
            });
          $fkTableContent.appendTo($newConvElement.find(".fkTableBody"));
        }
      }
      $newConvElement.find(".fkTableBody").find("tr").eq(0).remove();

      $newConvElement.find(".indexesCard").removeClass("template");
      $newConvElement.find(".indexFont").attr("href", "#indexKey" + i);
      $newConvElement.find(".indexFont").html("Secondary Indexes");
      $newConvElement.find(".indexTableBody").attr("id", "indexTableBody" + i);
      $newConvElement.find(".newIndexButton").attr("id", "indexButton" + i);
      $newConvElement
        .find(".collapse.indexCollapse")
        .attr("id", "indexKey" + i);
      $newConvElement
        .find(".index-acc-table.fkTable")
        .css("visibility", "hidden");
      $newConvElement
        .find(".index-acc-table.fkTable")
        .addClass("importantRule0");
      $newConvElement
        .find(".index-acc-table.fkTable")
        .removeClass("importantRule100");
      if (spTable.Indexes != null && spTable.Indexes.length != 0) {
        let indexKeys;
        $newConvElement
          .find(".index-acc-table.fkTable")
          .css("visibility", "visible");
        $newConvElement
          .find(".index-acc-table.fkTable")
          .addClass("importantRule100");
        $newConvElement
          .find(".index-acc-table.fkTable")
          .removeClass("importantRule0");
        for (var p = 0; p < spTable.Indexes.length; p++) {
          $indexTableContent = $newConvElement
            .find(".indexTableTr.template")
            .clone()
            .removeClass("template");
          $indexTableContent
            .find(".renameSecIndex.template")
            .attr("id", "renameSecIndex" + i + p);
          $indexTableContent
            .find(".saveSecIndex.template")
            .removeClass("template")
            .attr("id", "saveSecIndex" + i + p)
            .html(spTable.Indexes[p].Name);
          $indexTableContent
            .find(".acc-table-td.indexesTable")
            .html(spTable.Indexes[p].Table);
          $indexTableContent
            .find(".acc-table-td.indexesUnique")
            .html(spTable.Indexes[p].Unique.toString());
          indexKeys = "";
          for (var k = 0; k < spTable.Indexes[p].Keys.length; k++) {
            indexKeys += spTable.Indexes[p].Keys[k].Col + ", ";
          }
          indexKeys = indexKeys.replace(/,\s*$/, "");
          $indexTableContent.find(".acc-table-td.indexesKeys").html(indexKeys);
          $indexTableContent
            .find("button")
            .attr("id", spTable.Name + p + "secIndex");
          $indexTableContent
            .find("#" + spTable.Name + p + "secIndex")
            .click(function () {
              let indexId = jQuery(this).attr("id");
              let secIndexTableNumber = parseInt(
                jQuery(this)
                  .closest(".indexCollapse.collapse")
                  .attr("id")
                  .match(/\d+/),
                10
              );
              localStorage.setItem("indexId", indexId);
              localStorage.setItem("secIndexTableNumber", secIndexTableNumber);
              jQuery("#secIndexDeleteWarning").modal();
            });
          $indexTableContent.appendTo($newConvElement.find(".indexTableBody"));
        }
      }
      $newConvElement.find(".indexTableBody").find("tr").eq(0).remove();
      $newConvElement.find(".summaryFont").attr("href", "#viewSummary" + i);
      $newConvElement
        .find(".collapse.innerSummaryCollapse")
        .attr("id", "viewSummary" + i);
      $newConvElement
        .find(".mdc-card.summary-content")
        .html(
          JSON.parse(localStorage.getItem("summaryReportContent"))
          [srcTableName[i]].split("\n")
            .join("<br />")
        );
      $newConvElement.appendTo("#reportDiv");
    }
    // showSnackbar('schema converted successfully !!', ' greenBg');
    // initSchemaScreenTasks();
    for (var i = 0; i < srcTableNum; i++) {
      let tableId = "#src-sp-table" + i;
      jQuery(tableId).DataTable();
    }
    for (var i = 0; i < spTable_num; i++) {
      let spTable =
        schemaConversionObj.SpSchema[
        Object.keys(schemaConversionObj.SpSchema)[i]
        ];
      let spTableCols = spTable.ColNames;
      let spTableColsLength = spTableCols.length;
      for (var j = 0; j < spTableColsLength; j++) {
        if (document.getElementById("spConstraint" + i + j) != null) {
          // if (jQuery('#src-sp-table' + i).find('#spConstraint' + i + j) != null) {
          new vanillaSelectBox("#spConstraint" + i + j, {
            placeHolder: spPlaceholder[i][j] + " constraints selected",
            maxWidth: 500,
            maxHeight: 300,
          });
        }
        if (document.getElementById("srcConstraint" + i + j) != null) {
          new vanillaSelectBox("#srcConstraint" + i + j, {
            placeHolder: srcPlaceholder[i][j] + " constraints selected",
            maxWidth: 500,
            maxHeight: 300,
          });
        }
      }
    }
    tooltipHandler();
  };

  render() {
    if (!this.data) {
      return;
    }
    const { currentTab } = this.data;
    let schemaConversionObj = JSON.parse(localStorage.getItem("conversionReportContent"));
    let tableNameArray = Object.keys(schemaConversionObj.SpSchema);
    this.innerHTML = `<div class="summary-main-content" id='schema-screen-content'>
        <div id="snackbar" style="z-index: 10000 !important; position: fixed;"></div>
       
        <div>
            <h4 class="report-header">Recommended Schema Conversion Report
                <button id="download-schema" class="download-button" onclick='downloadSession()'>Download Session
                    File</button>
            </h4>
        </div>
        <div class="report-tabs">
        <ul class="nav nav-tabs md-tabs" role="tablist">
      ${TAB_CONFIG_DATA.map((tab) => {
      return `<hb-tab open=${currentTab === tab.id} id="${tab.id}" text="${tab.text}"></hb-tab>`;
    }).join("")} 
        </ul>
    </div>
        <div class="status-icons">
            <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm" id='reportSearchForm'>
                <i class="fas fa-search" aria-hidden="true"></i>
                <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table"
                    autocomplete='off' aria-label="Search" onkeyup='searchTable("reportSearchInput")'
                    id='reportSearchInput'>
            </form>
            <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm"
                style='display: none !important;' id='ddlSearchForm'>
                <i class="fas fa-search" aria-hidden="true"></i>
                <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table"
                    id='ddlSearchInput' autocomplete='off' aria-label="Search" onkeyup='searchTable("ddlSearchInput")'>
            </form>
            <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm"
                style='display: none !important;' id='summarySearchForm'>
                <i class="fas fa-search" aria-hidden="true"></i>
                <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table"
                    id='summarySearchInput' autocomplete='off' aria-label="Search"
                    onkeyup='searchTable("summarySearchInput")'>
            </form>
            <section class="cus-tip">
                <span class="cus-a info-icon statusTooltip">
                    <i class="large material-icons">info</i>
                    <span class="legend-icon statusTooltip"
                        style='cursor: pointer;display: inline-block;vertical-align: super;'>Status&nbsp;&nbsp;Legend</span>
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
                        <span class="avg"></span>
                        Average
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
            ${
      currentTab === "reportTab"
        ? `<div id="report" class="tab-pane fade show active">
        <div class="accordion md-accordion" id="accordion" role="tablist" aria-multiselectable="true">
            <button class='expand' id='reportExpandButton' onclick='reportExpandHandler(jQuery(this))'>Expand
                All</button>
            <button class='expand right-align' id='editButton' onclick='globalEditHandler()'>Edit Global Data
                Type</button>
            <div id='reportDiv'>

            ${tableNameArray.map((tableName) => {
              return `
              <hb-table-carousel title="${tableName} tabelId="report"></hb-table-carousel>
              `;
            }).join("")} 
                                
            </div>
        </div>
    </div>`
        : ""
      }
            ${
      currentTab === "ddlTab"
        ? `
        <div id="ddl" class="tab-pane fade show active">
                <div class="panel-group" id="ddl-accordion">
                    <button class='expand' id='ddlExpandButton' onclick='ddlExpandHandler(jQuery(this))'>Expand
                        All</button>
                    <button id="download-ddl" class="expand right-align" onclick='downloadDdl()'>Download DDL
                        Statements</button>
                    <div id='ddlDiv'>
                    ${tableNameArray.map((tableName) => {
                      return `
                              <hb-table-carousel title="${tableName}" tabelId="ddl"></hb-table-carousel>
                               `;
                    }).join("")} 
                                        
                    </div>
                  </div>
        </div>
                    
        `
        : ""
      }
            ${
      currentTab === "summaryTab"
        ? `
        <div id="summary" class="tab-pane fade show active">
        <div class="panel-group" id="summary-accordion">
            <button class='expand' id='summaryExpandButton' onclick='summaryExpandHandler(jQuery(this))'>Expand
                All</button>
            <button id="download-report" class="expand right-align" onclick='downloadReport()'>Download Summary
                Report</button>
            <div id='summaryDiv'>
            ${tableNameArray.map((tableName) => {
              return `
                       <hb-table-carousel title="${tableName}" tabelId="summary"></hb-table-carousel>
                      `;
            }).join("")} 
                                
            </div>
            </div>
            </div>

        `
        : ""
      }
            </div>
        </div>
    </div>
    <div class="modal" id="globalDataTypeModal" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true" data-backdrop="static" data-keyboard="false">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <!-- Modal content-->
            <div class="modal-content">
                <div class="modal-header content-center">
                    <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Global Data Type Mapping</h5>
                    <i class="large material-icons close" data-dismiss="modal">cancel</i>
                </div>
                <div class="modal-body" style='margin: auto; margin-top: 20px;'>
                    <div class="dataMappingCard" id='globalDataType'>
                        <table class='data-type-table' id='globalDataTypeTable'>
                            <tbody id='globalDataTypeBody'>
                                <tr>
                                    <th>Source</th>
                                    <th>Spanner</th>
                                </tr>
                                <tr class='globalDataTypeRow template'>
                                    <td class='src-td'></td>
                                    <td id='globalDataTypeCell'>
                                        <div style='display: flex;'>
                                            <i class="large material-icons warning" style='cursor: pointer;'>warning</i>
                                            <select class='form-control tableSelect' style='border: 0px !important;'>
                                                <option class='dataTypeOption template'></option>
                                            </select>
                                        </div>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div class="modal-footer" style='margin-top: 20px;'>
                    <button id="data-type-button" data-dismiss="modal" onclick="setGlobalDataType()" class="connectButton"
                        type="button" style='margin-right: 24px !important;'>Next</button>
                </div>
            </div>
        </div>
    </div>
    
    <div class="modal" id="foreignKeyDeleteWarning" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true" data-backdrop="static" data-keyboard="false" style="z-index: 999999;">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <!-- Modal content-->
            <div class="modal-content">
                <div class="modal-header content-center">
                    <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Warning</h5>
                    <i class="large material-icons close" data-dismiss="modal">cancel</i>
                </div>
                <div class="modal-body" style='margin-bottom: 20px; display: inherit;'>
                    <div><i class="large material-icons connectionFailure" style="color: #E1AD01D4 !important;">warning</i>
                    </div>
                    <div id="failureContent">
                        This will permanently delete the foreign key constraint and the corresponding uniqueness constraints
                        on referenced columns. Do you want to continue?
                    </div>
                </div>
                <div class="modal-footer">
                    <button data-dismiss="modal" class="connectButton" type="button"
                        onclick="dropForeignKeyHandler()">Yes</button>
                    <button data-dismiss="modal" class="connectButton" type="button">No</button>
                </div>
            </div>
        </div>
    </div>
    
    <div class="modal" id="secIndexDeleteWarning" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true" data-backdrop="static" data-keyboard="false" style="z-index: 999999;">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <!-- Modal content-->
            <div class="modal-content">
                <div class="modal-header content-center">
                    <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Warning</h5>
                    <i class="large material-icons close" data-dismiss="modal">cancel</i>
                </div>
                <div class="modal-body" style='margin-bottom: 20px; display: inherit;'>
                    <div><i class="large material-icons connectionFailure" style="color: #E1AD01D4 !important;">warning</i>
                    </div>
                    <div id="failureContent">
                        This will permanently delete the secondary index and the corresponding uniqueness constraints on
                        indexed columns (if applicable). Do you want to continue?
                    </div>
                </div>
                <div class="modal-footer">
                    <button data-dismiss="modal" class="connectButton" type="button"
                        onclick="dropSecondaryIndexHandler()">Yes</button>
                    <button data-dismiss="modal" class="connectButton" type="button">No</button>
                </div>
            </div>
        </div>
    </div>
    
    <div class="modal" id="editTableWarningModal" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true" data-backdrop="static" data-keyboard="false" style="z-index: 999999;">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <!-- Modal content-->
            <div class="modal-content">
                <div class="modal-header content-center">
                    <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Error Message</h5>
                    <i class="large material-icons close" data-dismiss="modal">cancel</i>
                </div>
                <div class="modal-body" style='margin-bottom: 20px; display: inherit;'>
                    <div><i class="large material-icons connectionFailure" style="color: #FF0000 !important;">cancel</i>
                    </div>
                    <div id="errorContent">
                    </div>
                </div>
                <div class="modal-footer">
                    <button data-dismiss="modal" class="connectButton" type="button">Ok</button>
                </div>
            </div>
        </div>
    </div>
    
    <div class="modal" id="editColumnNameErrorModal" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true" data-backdrop="static" data-keyboard="false" style="z-index: 999999;">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <!-- Modal content-->
            <div class="modal-content">
                <div class="modal-header content-center">
                    <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Error Message</h5>
                    <i class="large material-icons close" data-dismiss="modal">cancel</i>
                </div>
                <div class="modal-body" style='margin-bottom: 20px; display: inherit;'>
                    <div><i class="large material-icons connectionFailure" style="color: #FF0000 !important;">cancel</i>
                    </div>
                    <div id="editColumnNameErrorContent">
                    </div>
                </div>
                <div class="modal-footer">
                    <button data-dismiss="modal" class="connectButton" type="button">Ok</button>
                </div>
            </div>
        </div>
    </div>
    
    <div class="modal" id="createIndexModal" role="dialog" tabindex="-1" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true" data-backdrop="static" data-keyboard="false" style="z-index: 999999;">
        <div class="modal-dialog modal-dialog-centered" role="document">
            <!-- Modal content-->
            <div class="modal-content">
                <div class="modal-header content-center">
                    <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Select keys for new index</h5>
                    <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
                </div>
                <div class="modal-body" style='margin-bottom: 20px;'>
    
    
                    <form id="createIndexForm">
                        <div class="form-group secIndexLabel">
                            <label for="indexName" class="bmd-label-floating" style="color: black; width: 452px;">Enter
                                secondary index name</label>
                            <input type="text" class="form-control" name="indexName" id="indexName" autocomplete="off"
                                onfocusout="validateInput(document.getElementById('indexName'), 'indexNameError')"
                                style="border: 1px solid black !important;">
                            <span class='formError' id='indexNameError'></span>
                        </div>
                        <div class="newIndexColumnList template">
                            <span class="orderId" style="visibility: hidden;">1</span><span class="columnName"></span>
    
                            <span class="bmd-form-group is-filled">
                                <div class="checkbox" style="float: right;">
                                    <label>
                                        <input type="checkbox" value="">
                                        <span class="checkbox-decorator"><span class="check"
                                                style="border: 1px solid black;"></span>
                                            <div class="ripple-container"></div>
                                        </span>
                                    </label>
                                </div>
    
                            </span>
                        </div>
                        <div id="newIndexColumnListDiv" style="max-height: 200px; overflow-y: auto; overflow-x: hidden;"></div>
                        <!-- <div style="display: inline-flex;">
                            <div class="pmd-chip">Example Chip <a class="pmd-chip-action" href="javascript:void(0);">
                                <i class="material-icons">close</i></a>
                            </div>
                        </div>
                        <br> -->
                        <div style="display: inline-flex;">
                            <span style="margin-top: 18px; margin-right: 10px;">Unique</span>
                            <label class="switch">
                                <input id="uniqueSwitch" type="checkbox">
                                <span class="slider round"></span>
                            </label>
                        </div>
                    </form>
    
    
                </div>
                <div class="modal-footer">
                    <input type="submit"
                        onclick="fetchIndexFormValues(document.getElementById('indexName').value, document.getElementById('uniqueSwitch').checked)"
                        id="createIndexButton" class="connectButton" value="Create" disabled>
                </div>
            </div>
        </div>
    </div>`;
    initSchemaScreenTasks();
      // this.createSourceAndSpannerTables();

  }

  constructor() {
    super();
  }
}

window.customElements.define(
  "hb-schema-conversion-screen",
  SchemaConversionScreen
);
