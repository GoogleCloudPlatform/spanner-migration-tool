class TableCarousel extends HTMLElement {
    static get observedAttributes() {
        return ["open"];
    }

    get open() {
        return this.getAttribute("open");
    }

    attributeChangedCallback(name, oldValue, newValue) {
        this.render();
    }

    connectedCallback() {
        this.render(); 
    }

    render() {
        // let { id, open, text } = this;
        this.innerHTML = ``;
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-table-carousel", TableCarousel);
