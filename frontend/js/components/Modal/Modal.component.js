import Actions from "../../services/Action.service.js";
import Forms from "../../services/Forms.service.js";
import "../../components/ConnectToDbForm/ConnectToDbForm.component.js";
import "../../components/LoadDbDumpForm/LoadDbDumpForm.component.js";
import "../../components/LoadSessionFileForm/LoadSessionFileForm.component.js";

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
        // let modalContent = document.getElementById(this.modalId);
        // modalContent.querySelector("i").addEventListener("click", () => {
        //     Forms.clearModal();
        // })
    }

    render() {
        let { modalId, title, content } = this;
            this.innerHTML = `
            <div class="modal loadDatabaseDumpModal" id="${modalId}" tabindex="-1" role="dialog"
            aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered" role="document">
              <!-- Modal content-->
              <div class="modal-content">
                <div class="modal-header content-center">
                  <h5 class="modal-title modal-bg" id="exampleModalLongTitle">${title}</h5>
                  <i class="large material-icons close" data-dismiss="modal">cancel</i>
                </div>
                ${content}
              </div>
            </div>
          </div>
            `;
    }

    constructor() {
        super();
        this.addEventListener('click', () => {Actions['closeModal']});
    }
}

window.customElements.define('hb-modal', Modal);
