import "./../../components/Tab/Tab.component.js";
import "./../../components/TableCarousel/TableCarousel.component.js";
import "./../../components/StatusLegend/StatusLegend.component.js";
import "./../../components/Search/Search.component.js";
import "./../../components/Button/SiteButton.component.js"
import { initSchemaScreenTasks } from "./../../helpers/SchemaConversionHelper.js";

// Services
import Store from "./../../services/Store.service.js";
import "../../services/Fetch.service.js";

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

  render() {
    if (!this.data) {
      return;
    }
    const { currentTab } = this.data;
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
        return `<hb-tab open=${currentTab === tab.id} id="${tab.id}" 
        text="${tab.text}"></hb-tab>`;
      }).join("")} 
        </ul>
    </div>
        <div class="status-icons">
           <hb-search tabId=${currentTab}></hb-search>
            <hb-status-legend></hb-status-legend>
        </div>
        <div class="tab-bg" id='tabBg'>
            <div class="tab-content">
              ${currentTab === "reportTab"
                  ? `<div id="report" class="tab-pane fade show active">
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
              </div>`: ""}

          ${currentTab === "ddlTab"? `
        <div id="ddl" class="tab-pane fade show active">
                <div class="panel-group" id="ddl-accordion">
                <hb-site-button buttonid="ddlExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
                <hb-site-button buttonid="download-ddl" classname="expand right-align" buttonaction="downloadDdl" text="Download DDL Statements"></hb-site-button>

                    <div id='ddlDiv'>
                    ${tableNameArray.map((tableName) => {
                        return `
                              <hb-table-carousel title="${tableName}" tableId="ddl"></hb-table-carousel>
                               `;}).join("")} 
                    </div>
                  </div>
        </div>`: ""}

            ${
              currentTab === "summaryTab"
                ? `
        <div id="summary" class="tab-pane fade show active">
        <div class="panel-group" id="summary-accordion">
        <hb-site-button buttonid="summaryExpandButton" classname="expand" buttonaction="expandAll" text="Expand All"></hb-site-button>
        <hb-site-button buttonid="download-report" classname="expand right-align" buttonaction="downloadReport" text="Download Summary Report"></hb-site-button>

            <div id='summaryDiv'>
            ${tableNameArray
              .map((tableName) => {
                return `
                       <hb-table-carousel title="${tableName}" tableId="summary"></hb-table-carousel>
                      `;
              })
              .join("")} 
                                
            </div>
            </div>
            </div>
        `
                : ""
            }
            </div>
            <h5 class="no-text" id="notFound">No Match Found</h5>
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
