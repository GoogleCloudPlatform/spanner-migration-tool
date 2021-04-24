import Actions from "../../services/Action.service.js";

class Tab extends HTMLElement {
  
  get tabId() {
    return this.getAttribute("tabid");
  }

  get text() {
    return this.getAttribute("text");
  }

  get open(){
    return this.getAttribute("open")
  }

  connectedCallback() {
    this.render();
  }

  render() {
    let { tabId, text,open } = this;
    this.innerHTML = `
    <li class="nav-item">
        <a class="nav-link ${open==="true" ? "active show" : ""}" id="${tabId}Tab">${text}</a>
    </li>`;
  }
  

  constructor() {
    super();
    this.addEventListener("click", () => Actions.switchCurrentTab(`${this.tabId}Tab`));
  }
}

window.customElements.define("hb-tab", Tab);
