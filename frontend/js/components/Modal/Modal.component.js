import "../../components/ConnectToDbForm/ConnectToDbForm.component.js";
import "../../components/LoadDbDumpForm/LoadDbDumpForm.component.js";
import "../../components/LoadSessionFileForm/LoadSessionFileForm.component.js";

const CONNECT_TO_DB_MODAL_BUTTONS = [{value: "Connect", id: "connect-button"}];
const LOAD_DB_DUMP_MODAL_BUTTONS = [{value: "Confirm", id: "load-connect-button"}];
const LOAD_SESSION_MODAL_BUTTONS = [{value: "Confirm", id: "import-button"}];
const CONNECTION_SUCCESS_MODAL = [{value: "Convert", id: "convert-button"}];

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

  connectedCallback() {
    this.render();
  }

  render() {
    let { modalId, title, content } = this;
    let modalButtons;
    switch(modalId) {
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
    }
    this.innerHTML = `
        <div class="modal" id="${modalId}" tabindex="-1" role="dialog">
          <div class="modal-dialog modal-dialog-centered" role="document">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title modal-bg">${title}</h5>
                <i class="large material-icons close" data-dismiss="modal">cancel</i>
              </div>
              <div class="modal-body">
                ${content}
              </div>
              <div class="modal-footer">
                  ${modalButtons.map((button) => {
                    return `
                      <input type="submit" value="${button.value}" id="${button.id}" class="modal-button" />`;
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