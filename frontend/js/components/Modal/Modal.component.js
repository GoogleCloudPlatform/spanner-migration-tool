import Actions from "../../services/Action.service.js";
import "../../components/ConnectToDbForm/ConnectToDbForm.component.js";
import "../../components/LoadDbDumpForm/LoadDbDumpForm.component.js";

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
        // document.getElementById(this.modalId).addEventListener('click', () => {this.clearModal()})
    }

    clearModal = () => {
        document.getElementsByClassName('formError').innerHTML = '';
        document.getElementsByClassName('db-input').value = '';
        document.getElementsByClassName('db-select-input').value = '';
        document.getElementsByClassName('load-db-input').value = '';
        document.getElementsByClassName('import-db-input').value = '';
        document.getElementById('upload_link').innerHTML = 'Upload File';
        document.getElementById('loadConnectButton').disabled = true;
        document.getElementById('connectButton').disabled = true;
        document.getElementById('importButton').disabled = true;
        document.getElementById('indexName').value = '';
        document.getElementById('createIndexButton').disabled = true;
        if (document.getElementById('sqlFields') != undefined)
          document.getElementById('sqlFields').style.display = 'none';
        if (document.getElementById('sqlFieldsButtons') != undefined)
          document.getElementById('sqlFieldsButtons').style.display = 'none';
      }

    render() {
        let { modalId, title, content } = this;
        console.log(content);
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
                <div class="modal-body">
                  ${content}
                </div>
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
