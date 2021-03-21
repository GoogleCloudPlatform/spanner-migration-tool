import Actions from "./../../services/Action.service.js";

class Tabb extends HTMLElement {
    connectedCallback() {
       // this.render();
    }

    static get observedAttributes() {
        return ['open'];
    }

    attributeChangedCallback(name, oldValue, newValue) {
        console.log('in the attr change -Tabb ', name, oldValue, newValue);
        this.render();
    }

    clickHandler() {
        Actions[this.clickAction]();
        this.render();
    }

    get open() {
        return this.getAttribute('open');
    }

    get something() {
        return this.getAttribute('something');
    }

    get clickAction() {
        return this.getAttribute('clickAction');
    }

    render() {
        this.innerHTML = `
            <div>
                <div>This is the Tabb component - ${this.openValue}</div>
                <div>Value os something is ${this.something}</div>
            </div>
        `;
    }

    constructor() {
        super();
        this.openValue = this.open;
        this.addEventListener('click', this.clickHandler);// Actions[this.clickAction]);
    }
}

window.customElements.define('hb-tabb', Tabb);
