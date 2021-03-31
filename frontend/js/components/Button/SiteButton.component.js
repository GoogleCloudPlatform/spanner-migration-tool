import Actions from "../../services/Action.service.js";

class SiteButton extends HTMLElement {

    get id() {
        return this.getAttribute("id");
    }

    get text() {
        return this.getAttribute("text");
    }

    connectedCallback() {
        this.render(); 
    }

    render() {
        let { id, open, text } = this;
        this.innerHTML = `<button class='expand' id='expand-btn' >Expand
        All</button>`;
    }

    constructor() {
        super();
        this.addEventListener("click", () => Actions.expandAll(document.getElementById('expand-btn').innerHTML.split('/n').join(" ")));
    }
}

window.customElements.define("hb-site-button", SiteButton);
