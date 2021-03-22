import Actions from "../../services/Action.service.js";

class Modal extends HTMLElement {

    // static get observedAttributes() {
    //     return ['open'];
    // }

    // get open() {
    //     return this.getAttribute('open');
    // }

    get title() {
        return this.getAttribute('title');
    }

    get id() {
        return this.getAttribute('id');
    }

    // attributeChangedCallback(name, oldValue, newValue) {
    //     this.render();
    // }

    connectedCallback() {
       this.render();
    }

    render() {
        let { id, title } = this;
        this.innerHTML = `

        <div class="modal" id="${id}" tabindex="-1" role="dialog" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true">
        <div class="modal-dialog modal-dialog-centered" role="document">
          <div class="modal-content">
            <div class="modal-header content-center">
                <h5 class="modal-title modal-bg" id="exampleModalLongTitle">"${title}"</h5>
                <!-- <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i> -->
            </div>
          </div>
        </div>
      </div>


        `;
    }

    // get, set is used to get the values of the attributes
    constructor() {
        super();
        // this.data = {open: this.open, name: "Hii"};
        // this.addEventListener('click', Actions.closeStore);
    }
}

window.customElements.define('hb-modal', Modal);
