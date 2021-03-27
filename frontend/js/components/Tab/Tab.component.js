import Actions from "../../services/Action.service.js";

class Tab extends HTMLElement {
    static get observedAttributes() {
        return ["open"];
    }

    get open() {
        return this.getAttribute("open");
    }

    get id() {
        return this.getAttribute("id");
    }

    get text() {
        return this.getAttribute("text");
    }

    attributeChangedCallback(name, oldValue, newValue) {
        this.render();
    }

    connectedCallback() {
        this.render(); 
    }

    render() {
        let { id, open, text } = this;
        this.innerHTML = `<li class="nav-item">
                        <p class="nav-link  ${open === "true" ? "active" : ""}" 
                        id=${id} href="#">${text}</p>
                      </li>`;
    }

    constructor() {
        super();
        this.addEventListener("click", () => Actions.switchToTab(this.id));
    }
}

window.customElements.define("hb-tab", Tab);
