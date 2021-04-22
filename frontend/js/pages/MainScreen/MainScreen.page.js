// Components
import "../../components/Tab/Tab.component.js";
import "../../components/Label/Label.component.js";
import "../../components/ImageIcon/ImageIcon.component.js";
import "../../components/Modal/Modal.component.js";
import "../../components/HistoryTable/HistoryTable.component.js";

// Services
import Store from "./../../services/Store.service.js";
import Actions from "../../services/Action.service.js";

//constants
import{MAIN_PAGE_ICONS,MAIN_PAGE_STATIC_CONTENT} from "./../../config/constantData.js";

//helper 
import {setActiveSelectedMenu} from './../../helpers/SchemaConversionHelper.js';

class MainScreen extends HTMLElement {
  connectedCallback() {
    this.stateObserver = setInterval(this.observeState, 200);
    this.render();
    Actions.resetStore();
    setActiveSelectedMenu('homeScreen')
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
    this.innerHTML = `
                        <div>
                          <div id="snackbar"></div>
                            <div class="page-heading">
                              <hb-label type="heading" text="${MAIN_PAGE_STATIC_CONTENT.HEADING_TEXT}"></hb-label>
                              <hb-label type="subHeading" text="${MAIN_PAGE_STATIC_CONTENT.SUB_HEADING_TEXT}"></hb-label>
                            </div>
                          <div class="card-area">
                            <div class="icons-card-section">
                              ${MAIN_PAGE_ICONS.map((icon) => { return `
                                 <div class="icon-card">
                                   <hb-image-icon image="${icon.image}" imageAltText="${icon.imageAltText}" label="${icon.label}" clickAction="${icon.action}" modalDataTarget="${icon.modalDataTarget}">
                                   </hb-image-icon>
                                 </div>
                               `; }).join("")}
                            </div>
                          </div>

                          <hb-modal
                            modalId="connectToDbModal"
                            content="<hb-connect-to-db-form></hb-connect-to-db-form>"
                            contentIcon=""
                            connectIconClass=""
                            modalBodyClass=""
                            title="Connect to Database">
                          </hb-modal>

                          <hb-modal
                            modalId="loadDatabaseDumpModal"
                            content="<hb-load-db-dump-form></hb-load-db-dump-form>"
                            contentIcon=""
                            connectIconClass=""
                            modalBodyClass=""
                            title="Load Database Dump">
                          </hb-modal>

                          <hb-modal
                            modalId="loadSchemaModal"
                            content="<hb-load-session-file-form></hb-load-session-file-form>"
                            contentIcon=""
                            connectIconClass=""
                            modalBodyClass=""
                            title="Load Session File">
                          </hb-modal>

                          <hb-modal
                            modalId="connectModalSuccess"
                            content="${MAIN_PAGE_STATIC_CONTENT.CONNECTION_SUCCESS_CONTENT}"
                            contentIcon="check_circle"
                            connectIconClass="connect-icon-success"
                            modalBodyClass="connection-modal-body"
                            title="Connection Successful">
                          </hb-modal>

                          <hb-modal
                            modalId="connectModalFailure"
                            content="${MAIN_PAGE_STATIC_CONTENT.CONNECTION_FAILURE_CONTENT}"
                            contentIcon="cancel"
                            connectIconClass="connect-icon-failure"
                            modalBodyClass="connection-modal-body"
                            title="Connection Failure">
                          </hb-modal>

                        <div class="history-content">
                         <hb-history-table></hb-history-table>
                        </div>
            
                    </div>` ;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-main-screen", MainScreen);
