import Actions from "../../services/Action.service.js";

class SiteButton extends HTMLElement {
  get buttonId() {
    return this.getAttribute("buttonid");
  }

  get text() {
    return this.getAttribute("text");
  }

  get className() {
    return this.getAttribute("classname");
  }

  get buttonAction() {
    return this.getAttribute("buttonaction");
  }

  connectedCallback() {
    this.render();
  }

  render() {
    this.innerHTML = `<button class="${this.className}" id="${this.buttonId}" >${this.text}</button>`;
  }

  constructor() {
    super();
    this.addEventListener("click", () => {
      switch (this.buttonAction) {
        case "expandAll":
          Actions[this.buttonAction](
            document.getElementById(this.buttonId).innerHTML,
            this.buttonId,
          );
          break;

        case "createNewSecIndex":
          Actions[this.buttonAction](this.buttonId);
          break;
          
        default:
            Actions[this.buttonAction]();
            break;

      }
    });
  }
}

window.customElements.define("hb-site-button", SiteButton);
