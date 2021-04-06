import "./../../components/Tab/Tab.component.js";
import "./../../components/TableCarousel/TableCarousel.component.js";
import "./../../components/StatusLegend/StatusLegend.component.js";
import "./../../components/Search/Search.component.js";
import "./../../components/Button/SiteButton.component.js"
import "../../components/EditGlobalDataTypeForm/EditGlobalDataTypeForm.componenet.js"
import "./../../components/Modal/Modal.component.js"
import { initSchemaScreenTasks } from "./../../helpers/SchemaConversionHelper.js";

// Services
import Store from "./../../services/Store.service.js";
import "../../services/Fetch.service.js";
import Actions from "../../services/Action.service.js";

const TAB_CONFIG_DATA = [
  {
    id: "report",
    text: "Conversion Report",
  },
  {
    id: "ddl",
    text: "DDL Statements",
  },
  {
    id: "summary",
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

    render() {
    if (!this.data) {
      return;
    }
     Actions.getGlobalDataTypeList()
    let schemaConversionObj = JSON.parse(
      localStorage.getItem("conversionReportContent")
    );
    let tableNameArray = Object.keys(schemaConversionObj.SpSchema);
    this.innerHTML = `<div class="summary-main-content" id='schema-screen-content'>
        <div id="snackbar" style="z-index: 10000 !important; position: fixed;"></div>
       
        <div>
            <h4 class="report-header">Recommended Schema Conversion Report
            <hb-site-button buttonid="download-schema" classname="download-button" buttonaction="downloadSession" text="Download Session File"></hb-site-button>
            </h4>
        </div>
        <div class="report-tabs">
        <ul class="nav nav-tabs md-tabs" role="tablist">

         ${TAB_CONFIG_DATA.map((tab) => {
                return `<hb-tab tabid="${tab.id}" text="${tab.text}"></hb-tab>`;
            }).join("")} 
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
              <div id="report" class="tab-pane fade show active">
                    <div class="accordion md-accordion" id="accordion" role="tablist" aria-multiselectable="true">
                        <hb-site-button buttonid="reportExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
                        <hb-site-button buttonid="editButton" classname="expand right-align" buttonaction="editGlobalDataType" text="Edit Global Data Type"></hb-site-button>

                            
            <div id='reportDiv'>
            ${tableNameArray.map((tableName, index) => {
                return `
                  <hb-table-carousel title="${tableName}" tableId="report" tableIndex="${index}"></hb-table-carousel>
              `; }).join("")}                    
            </div>
        </div>
        <h5 class="no-text" id="reportnotFound">No Match Found</h5>
              </div>
              

          
        <div id="ddl" class="tab-pane fade">
                <div class="panel-group" id="ddl-accordion">
                <hb-site-button buttonid="ddlExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
                <hb-site-button buttonid="download-ddl" classname="expand right-align" buttonaction="downloadDdl" text="Download DDL Statements"></hb-site-button>

                    <div id='ddlDiv'>
                    ${tableNameArray.map((tableName,index) => {
                        return `
                              <hb-table-carousel title="${tableName}" tableId="ddl" tableIndex=${index}></hb-table-carousel>
                               `;}).join("")} 
                    </div>
                  </div>
                  <h5 class="no-text" id="ddlnotFound">No Match Found</h5>
        </div>

            
        <div id="summary" class="tab-pane fade">
        <div class="panel-group" id="summary-accordion">
        <hb-site-button buttonid="summaryExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
        <hb-site-button buttonid="download-report" classname="expand right-align" buttonaction="downloadReport" text="Download Summary Report"></hb-site-button>

            <div id='summaryDiv'>
            ${tableNameArray
              .map((tableName,index) => {
                return `
                       <hb-table-carousel title="${tableName}" tableId="summary" tableIndex=${index}></hb-table-carousel>
                      `;
              })
              .join("")} 
                                
            </div>
        </div>
            <h5 class="no-text" id="summarynotFound">No Match Found</h5>
            </div>
            </div>
        </div>
    </div>

    <hb-modal modalId="globalDataTypeModal" 
    content="<hb-edit-global-datatype-form></hb-edit-global-datatype-form>" 
    contentIcon="" 
    connectIconClass="" modalBodyClass="" title="Global Data Type Mapping"></hb-modal>
    
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
    <hb-modal modalId="createIndexModal" 
    content=""
    contentIcon="" connectIconClass="" modalBodyClass="" 
    title="Select keys for new index"></hb-modal>
`;
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
