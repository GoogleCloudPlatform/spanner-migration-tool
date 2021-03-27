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
  }

  render() {
    let { modalId, title, content } = this;
    this.innerHTML = `
        <div class="modal" id="${modalId}" tabindex="-1" role="dialog">
          <div class="modal-dialog modal-dialog-centered" role="document">
            <div class="modal-content">
              <div class="modal-header">
                <h5 class="modal-title modal-bg">${title}</h5>
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
  }
}

window.customElements.define('hb-modal', Modal);
