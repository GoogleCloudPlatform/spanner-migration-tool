import "./../../components/Tab/Tab.component.js";
import "./../../components/TableCarousel/TableCarousel.component.js";
import "./../../components/StatusLegend/StatusLegend.component.js";
import "./../../components/Search/Search.component.js";
import "../../components/SiteButton/SiteButton.component.js";
import "../../components/EditGlobalDataTypeForm/EditGlobalDataTypeForm.componenet.js";
import "./../../components/Modal/Modal.component.js";
import { initSchemaScreenTasks } from "./../../helpers/SchemaConversionHelper.js";

// Services
import Store from "./../../services/Store.service.js";
import Actions from "./../../services/Action.service.js";
import "../../services/Fetch.service.js";

// constants
import { TAB_CONFIG_DATA } from "./../../config/constantData.js";

class SchemaConversionScreen extends HTMLElement {
  connectedCallback() {
    this.stateObserver = setInterval(this.observeState, 200);
    Actions.getGlobalDataTypeList();
    this.render();
  }

  disconnectedCallback() {
    clearInterval(this.stateObserver);
  }

  sendDatatoReportTab(tableNameArray, currentTabContent) {
      for (let i = 0; i < tableNameArray.length; i++) {
        let filterdata = {
          SpSchema: currentTabContent.SpSchema[tableNameArray[i]],
          SrcSchema: currentTabContent.SrcSchema[tableNameArray[i]],
          ToSource: currentTabContent.ToSource[tableNameArray[i]],
          ToSpanner: currentTabContent.ToSpanner[tableNameArray[i]],
          summary : Store.getinstance().tableData["summaryTabContent"][tableNameArray[i]],
        };
        let component = document.querySelector(`#reportTab${i}`);
        // component.setAttribute('data',JSON.stringify(filterdata));
        component.data = filterdata;
      }
  }

  getChangingValue(currentTab) {
    currentTab = currentTab.substring(0, currentTab.length - 3);
    let currentArray = Store.getinstance().openStatus[currentTab];
    let flag = "Expand All";
    for (let i = 0; i < currentArray.length; i++) {
      if (currentArray[i] == true) {
        flag = "Collapse All";
      }
    }
    return flag;
  }

  observeState = () => {
    let updatedData = Store.getinstance();
    if (JSON.stringify(updatedData) !== JSON.stringify(this.data)) {
      console.log("in the if..");
      this.data = JSON.parse(JSON.stringify(updatedData));
      this.render();
      Actions.ddlSummaryAndConversionApiCall();
    }
    if (Store.getTableChanges() == "saveMode" && JSON.stringify(updatedData) == JSON.stringify(this.data)) {
      Store.setTableChanges("editMode");
      this.render();
    }
  };

  render() {
    console.log(this.data);
    if (!this.data) {
      return;
    }
    const { currentTab, tableData, tableBorderData,searchInputValue } = this.data;
    console.log(searchInputValue);
    let currentTabContent = tableData[`${currentTab}Content`];
    const changingText = this.getChangingValue(currentTab);
    let tableNameArray;
    if (currentTab === "reportTab") {
      tableNameArray = Object.keys(currentTabContent.SpSchema);
      // delete currentTabContent["Stats"];
    } else {
      tableNameArray = Object.keys(currentTabContent);
    }

    this.innerHTML = `
    <div class="summary-main-content" id='schema-screen-content'>
      <div id="snackbar" class="schema-screen-snackbar"></div>
      <div>
        <h4 class="report-header">Recommended Schema Conversion Report
            <hb-site-button buttonid="download-schema" classname="download-button" 
                buttonaction="downloadSession" text="Download Session File"></hb-site-button>
        </h4>
      </div>
      <div class="report-tabs">
        <ul class="nav nav-tabs md-tabs" role="tablist">
           ${TAB_CONFIG_DATA.map((tab) => {
             return `<hb-tab open="${
               Store.getinstance().currentTab === `${tab.id}Tab`
             }" tabid="${tab.id}" text="${tab.text}"></hb-tab>`;
           }).join("")} 
        </ul>
      </div>
      <div class="status-icons">
        <hb-search tabid="${currentTab}" searchid="search-input" id="reportSearchForm" class="inlineblock" ></hb-search>
        <hb-status-legend></hb-status-legend> 
      </div> 
      <div class="tab-bg" id='tabBg'>
        <div class="tab-content">
          ${currentTab === 'reportTab' ? `<div id="report" class="tab-pane fade show active">
            <div class="accordion md-accordion" id="accordion" role="tablist" aria-multiselectable="true">
              <hb-site-button buttonid="reportExpandButton" classname="expand" buttonaction="expandAll" text="${changingText}"></hb-site-button>
              <hb-site-button buttonid="editButton" classname="expand right-align" buttonaction="editGlobalDataType" text="Edit Global Data Type"></hb-site-button>
              <div id='reportDiv'>
                ${tableNameArray.filter((title)=>title.indexOf(searchInputValue[currentTab]) > -1)
                  .map((tableName, index) => {
                    return `
                    <hb-table-carousel tableTitle="${tableName}" id="${currentTab}${index}" tabId="report" 
                    tableIndex="${index}" borderData = "${tableBorderData[tableName]}"></hb-table-carousel>`;
                  })
                  .join("")}                    
                </div>
            </div>
            <h5 class="no-text" id="reportnotFound">No Match Found</h5>
          </div>`
              : `<div></div>`
          }

          ${
            currentTab === "ddlTab"
              ? `<div id="ddl" class="tab-pane fade show active">
            <div class="panel-group" id="ddl-accordion">
              <hb-site-button buttonid="ddlExpandButton" classname="expand" buttonaction="expandAll" text="${changingText}"></hb-site-button>
              <hb-site-button buttonid="download-ddl" classname="expand right-align" buttonaction="downloadDdl" text="Download DDL Statements"></hb-site-button>
              <div id='ddlDiv'>
                ${tableNameArray.filter((title)=>title.indexOf(searchInputValue[currentTab]) > -1)
                  .map((tableName, index) => {
                    return `
                    <hb-table-carousel tableTitle="${tableName}" stringData="${currentTabContent[tableName]}" tabId="ddl" id="${currentTab}${index}" tableIndex=${index} borderData = "${tableBorderData[tableName]}">
                    </hb-table-carousel>`;
                  })
                  .join("")} 
                </div>
            </div>
            <h5 class="no-text" id="ddlnotFound">No Match Found</h5>
          </div>`
              : `<div></div>`
          }

          ${
            currentTab === "summaryTab"
              ? `<div id="summary" class="tab-pane fade show active">
            <div class="panel-group" id="summary-accordion">
              <hb-site-button buttonid="summaryExpandButton" classname="expand" buttonaction="expandAll" text="${changingText}"></hb-site-button>
              <hb-site-button buttonid="download-report" classname="expand right-align" buttonaction="downloadReport" text="Download Summary Report"></hb-site-button>
              <div id='summaryDiv'>
                ${tableNameArray.filter((title)=>title.indexOf(searchInputValue[currentTab]) > -1)
                  .map((tableName, index) => {
                    return `
                    <hb-table-carousel tableTitle="${tableName}" stringData="${currentTabContent[tableName]}" id="${currentTab}${index}" tabId="summary" 
                    tableIndex=${index} borderData = "${tableBorderData[tableName]}"></hb-table-carousel>`;
                  })
                  .join("")} 
              </div>
            </div>
            <h5 class="no-text" id="summarynotFound">No Match Found</h5>
          </div>`
              : `<div></div>`
          }

          </div>
      </div>
    </div>
    <hb-modal modalId="globalDataTypeModal" content="<hb-edit-global-datatype-form></hb-edit-global-datatype-form>" 
      contentIcon="" connectIconClass="" modalBodyClass="" title="Global Data Type Mapping"></hb-modal>
    <hb-modal modalId="indexAndKeyDeleteWarning" content="" contentIcon="warning" 
      connectIconClass="warning-icon" modalBodyClass="connection-modal-body" title="Warning"></hb-modal>
    <hb-modal modalId="editTableWarningModal" content="edit table" contentIcon="cancel" 
      connectIconClass="connect-icon-failure" modalBodyClass="connection-modal-body" title="Error Message"></hb-modal>
    <hb-modal modalId="createIndexModal" content="" contentIcon="" 
      connectIconClass="" modalBodyClass="" title="Select keys for new index"></hb-modal> `;
    initSchemaScreenTasks();
    if (currentTab === "reportTab") {
      this.sendDatatoReportTab(tableNameArray.filter((title)=>title.indexOf(searchInputValue[currentTab]) > -1), currentTabContent);
    }
  }

  constructor() {
    super();
  }
}

window.customElements.define(
  "hb-schema-conversion-screen",
  SchemaConversionScreen
);
