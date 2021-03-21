import Actions from "../../services/Action.service.js";

class Tab extends HTMLElement {

    // The standard to mention what you are observing
    static get observedAttributes() {
        return ['disabled', 'open'];
    }

    get open() {
        return this.getAttribute('open');
    }

    attributeChangedCallback(name, oldValue, newValue) {
        this.render();
    }

    connectedCallback() {
     //   this.render();
    }

    render() {
        let {name, open} = this.data;
        this.innerHTML = `
            <div>This is the ${name}, And this does ${open} </div>
        `;
    }

    // get, set is used to get the values of the attributes
    constructor() {
        super();
        this.data = {open: this.open, name: "Hii"};
        this.addEventListener('click', Actions.closeStore);
    }
}

window.customElements.define('hb-tab', Tab);
