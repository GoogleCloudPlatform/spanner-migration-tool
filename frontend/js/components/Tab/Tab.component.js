import Actions from "../../services/Action.service.js";

class Tab extends HTMLElement {
  get tabId() {
    return this.getAttribute("tabid");
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
    let { tabId, text } = this;
    this.innerHTML = `<li class="nav-item">
                        <a class="nav-link ${
                          tabId === "report" ? "active show" : ""
                        }" id="${tabId}Tab">${text}</a>
                      </li>`;
  }

  constructor() {
    super();
    this.addEventListener("click", () => Actions.switchToTab(this.tabId));
  }
}

window.customElements.define("hb-tab", Tab);
