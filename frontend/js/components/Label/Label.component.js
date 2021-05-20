import {CLASS_NAMES} from "./../../config/constantData.js";
class Label extends HTMLElement {

    get type() {
        return this.getAttribute('type');
    }

    get text() {
        return this.getAttribute('text');
    }

    connectedCallback() {
        this.render();
    }

    render() {
        let { type, text } = this;
        let className = CLASS_NAMES[type] || 'text';
        this.innerHTML = `
            <div class="label ${className}">${text}</div>
        `;  
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-label', Label);