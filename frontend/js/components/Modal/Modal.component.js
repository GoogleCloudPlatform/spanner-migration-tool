import "../../components/ConnectToDbForm/ConnectToDbForm.component.js";
import "../../components/LoadDbDumpForm/LoadDbDumpForm.component.js";
import "../../components/LoadSessionFileForm/LoadSessionFileForm.component.js";
import "../../components/AddIndexForm/AddIndexForm.component.js";

const CONNECT_TO_DB_MODAL_BUTTONS = [{ value: "Connect", id: "connect-button", disabledProp: "disabled" }];
const LOAD_DB_DUMP_MODAL_BUTTONS = [{ value: "Confirm", id: "load-connect-button", disabledProp: "disabled" }];
const LOAD_SESSION_MODAL_BUTTONS = [{ value: "Confirm", id: "import-button", disabledProp: "disabled" }];
const CONNECTION_SUCCESS_MODAL = [{ value: "Convert", id: "convert-button", disabledProp: "" }];
const CONNECTION_FAILURE_MODAL = [{ value: "Ok", id: "connection-failure-button", disabledProp: "" }];
const EDIT_GLOBAL_DATATYPE_MODAL = [{ value: "Next", id: "data-type-button", disabledProp: "" }];
const ADD_INDEX_MODAL = [{ value: "CREATE", id: "createIndexButton", disabledProp: "disabled" }];
const EDIT_TABLE_WARNING_MODAL = [{ value: "Ok", id: "edit-table-warning", disabledProp: "" }];
const FK_DROP_WARNING_MODAL = [{ value: "Yes", id: "fk-drop-confirm", disabledProp: "" }, { value: "No", id: "fk-drop-cancel", disabledProp: "" }];

class Modal extends HTMLElement {

  get modalId() {
    return this.getAttribute('modalId');
  }
  get title() {
    return this.getAttribute('title');
  }
  get content() {
    return this.getAttribute('content');
  }
  get contentIcon() {
    return this.getAttribute('contentIcon');
  }
  get modalBodyClass() {
    return this.getAttribute('modalBodyClass');
  }
  get connectIconClass() {
    return this.getAttribute('connectIconClass');
  }


  static get observedAttributes() {
    return ['content'];
  }

  attributeChangedCallback(attrName, oldVal, newVal) {
    if (oldVal !== newVal) {

      this.render();
    }
  }

  connectedCallback() {
    this.render();
  }

  render() {
    let { modalId, title, content, contentIcon, modalBodyClass, connectIconClass } = this;
    let modalButtons;
    switch (modalId) {
      case "connectToDbModal":
        modalButtons = CONNECT_TO_DB_MODAL_BUTTONS;
        break;
      case "loadDatabaseDumpModal":
        modalButtons = LOAD_DB_DUMP_MODAL_BUTTONS;
        break;
      case "loadSchemaModal":
        modalButtons = LOAD_SESSION_MODAL_BUTTONS;
        break;
      case "connectModalSuccess":
        modalButtons = CONNECTION_SUCCESS_MODAL;
        break;
      case "connectModalFailure":
        modalButtons = CONNECTION_FAILURE_MODAL;
        break;
      case "globalDataTypeModal":
        modalButtons = EDIT_GLOBAL_DATATYPE_MODAL;
        break;
      case "createIndexModal":
        modalButtons = ADD_INDEX_MODAL;
        break;
      case "editTableWarningModal":
        modalButtons = EDIT_TABLE_WARNING_MODAL;
        break;
      case "indexAndKeyDeleteWarning":
        modalButtons = FK_DROP_WARNING_MODAL;
        break;
    }

    this.innerHTML = `
        <div class="modal" id="${modalId}" tabindex="-1" role="dialog" data-backdrop="static" data-keyboard="false">
          <div class="modal-dialog modal-dialog-centered" role="document">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title modal-bg">${title}</h5>
                <i class="large material-icons close" data-dismiss="modal">cancel</i>
              </div>

              <div class="modal-body ${modalBodyClass}">
                <div><i class="large material-icons ${connectIconClass}">${contentIcon}</i></div>
                <div id="modal-content">${content.trim()}</div>
              </div>
              <div class="modal-footer">
                  ${modalButtons.map((button) => {
      return `
                      <input type="submit" ${button.disabledProp} value="${button.value}" id="${button.id}" ${button.modalDismiss?"data-dismiss='modal'":""} class="modal-button" />`;
    }).join("")}
              </div>
            </div>
          </div>
        </div>
      `;

  }

  constructor() {
    super();
  }
}

window.customElements.define('hb-modal', Modal);
