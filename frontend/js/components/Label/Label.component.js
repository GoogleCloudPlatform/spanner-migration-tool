/**
 *  Label will carry type as an attribute - 
 * Enum => heading, subheading, text
 */
class Label extends HTMLElement {

    connectedCallback() {
        this.render();
    }

    get type() {
        return this.getAttribute('type');
    }

    get text() {
        return this.getAttribute('text');
    }

    render() {
        let { type, text } = this;
        let className = type === "heading" ? "heading" : (type === "subHeading" ? "sub-heading" : "text");
        this.innerHTML = `
            <div class="label ${className}">${text}</div>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-label', Label);