import Actions from "../../services/Action.service.js";

class Tab extends HTMLElement {

    static get observedAttributes() {
        return ['open'];
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
        let textClass = open === 'no' ? 'redd' : 'greenn';
        this.innerHTML = `
            <div class="${textClass}">This is the ${name}, And this does ${open} </div>
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
