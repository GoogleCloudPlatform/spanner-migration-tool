import "./../../components/Tab/Tab.component.js";
import "./../../components/TableCarousel/TableCarousel.component.js";
import "./../../components/StatusLegend/StatusLegend.component.js";
import "./../../components/Search/Search.component.js";
import "../../components/SiteButton/SiteButton.component.js"
import "../../components/EditGlobalDataTypeForm/EditGlobalDataTypeForm.componenet.js"
import "./../../components/Modal/Modal.component.js"
import { initSchemaScreenTasks } from "./../../helpers/SchemaConversionHelper.js";

// Services
import Store from "./../../services/Store.service.js";
import Actions from "./../../services/Action.service.js";
import "../../services/Fetch.service.js";

// constants
import {TAB_CONFIG_DATA} from './../../config/constantData.js'


class SchemaConversionScreen extends HTMLElement {
  connectedCallback() {
    this.stateObserver = setInterval(this.observeState, 200);
    Actions.getGlobalDataTypeList();
    this.render();
  }

  disconnectedCallback() {
    clearInterval(this.stateObserver);
  }

  observeState = () => {
    let updatedData = Store.getinstance();
    console.log(this.data,updatedData);
    if (JSON.stringify(updatedData) !== JSON.stringify(this.data)) {
      console.log('in the if..');
      this.data = updatedData;
      this.render();
      Actions.ddlSummaryAndConversionApiCall();
    }
  };

  render() {
    if (!this.data) {
      return;
    }
    const {currentTab , tableData, tableBorderData} = this.data;
    const reportTabContent = tableData['reportTabContent'] 
    const ddlTabContent = tableData['ddlTabContent'] 
    const summaryTabContent = tableData['summaryTabContent'] 
    let tableNameArray = Object.keys(reportTabContent.SpSchema)
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
                return `<hb-tab open="${Store.getinstance().currentTab === `${tab.id}Tab`}" tabid="${tab.id}" text="${tab.text}"></hb-tab>`}).join("")} 
        </ul>
      </div>
      <div class="status-icons">
        <hb-search tabid="report" searchid="reportSearchInput" id="reportSearchForm" class="inlineblock" ></hb-search>
        <hb-search tabid="ddl" searchid="ddlSearchInput" id="ddlSearchForm" class="template"></hb-search>
        <hb-search tabid="summary" searchid="summarySearchInput" id="summarySearchForm" class="template"></hb-search>
        <hb-status-legend></hb-status-legend>
      </div> 
      <div class="tab-bg" id='tabBg'>
        <div class="tab-content">
          ${currentTab === 'reportTab' ? `<div id="report" class="tab-pane fade show active">
            <div class="accordion md-accordion" id="accordion" role="tablist" aria-multiselectable="true">
              <hb-site-button buttonid="reportExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
              <hb-site-button buttonid="editButton" classname="expand right-align" buttonaction="editGlobalDataType" text="Edit Global Data Type"></hb-site-button>
              <div id='reportDiv'>
                ${tableNameArray.map((tableName, index) => { return `
                    <hb-table-carousel tableTitle="${tableName}" data="{}" id="${currentTab}${index}" tableId="report" 
                    tableIndex="${index}"></hb-table-carousel>`}).join("")}                    
                </div>
            </div>
            <h5 class="no-text" id="reportnotFound">No Match Found</h5>
          </div>` : `<div></div>`}

          ${currentTab === 'ddlTab' ? `<div id="ddl" class="tab-pane fade show active">
            <div class="panel-group" id="ddl-accordion">
              <hb-site-button buttonid="ddlExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
              <hb-site-button buttonid="download-ddl" classname="expand right-align" buttonaction="downloadDdl" text="Download DDL Statements"></hb-site-button>
              <div id='ddlDiv'>
                ${tableNameArray.map((tableName,index) => {return `
                    <hb-table-carousel tableTitle="${tableName}" data="{}" tableId="ddl" id="${currentTab}${index}" tableIndex=${index}>
                    </hb-table-carousel>`;}).join("")} 
                </div>
            </div>
            <h5 class="no-text" id="ddlnotFound">No Match Found</h5>
          </div>`:`<div></div>`}
          ${currentTab === 'summaryTab' ? `<div id="summary" class="tab-pane fade show active">
            <div class="panel-group" id="summary-accordion">
              <hb-site-button buttonid="summaryExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
              <hb-site-button buttonid="download-report" classname="expand right-align" buttonaction="downloadReport" text="Download Summary Report"></hb-site-button>
              <div id='summaryDiv'>
                ${tableNameArray.map((tableName,index) => {return `
                    <hb-table-carousel tableTitle="${tableName}" data="{}" id="${currentTab}${index}" tableId="summary" 
                    tableIndex=${index}></hb-table-carousel>`;}).join("")} 
              </div>
            </div>
            <h5 class="no-text" id="summarynotFound">No Match Found</h5>
          </div>`:`<div></div>`}
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
      let data;
      switch(currentTab)
      {
        case "reportTab":
          data = { content: reportTabContent, borderData : tableBorderData }
          break;
        case "ddlTab":
          data ={ content: ddlTabContent, borderData : tableBorderData }
          break;
        case "summaryTab":
          data ={ content: summaryTabContent, borderData : tableBorderData }
          break;
      }
      
      for(let i=0;i<tableNameArray.length;i++)
      {
        let filterdata;

        if(currentTab === 'reportTab')
        {
        filterdata = {content:data.content.SpSchema[tableNameArray[i]],borderData:data.borderData[tableNameArray[i]]}
         }
        else{
       filterdata = {content:data.content[tableNameArray[i]],borderData:data.borderData[tableNameArray[i]] }
      }
        let component = document.querySelector(`#${currentTab}${i}`)
        component.setAttribute('data',JSON.stringify(filterdata))
        
      }


  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-schema-conversion-screen",SchemaConversionScreen);
