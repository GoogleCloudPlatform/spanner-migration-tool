import {NAVLINKS} from "../../config/constantData.js";

class Header extends HTMLElement {
  connectedCallback() {
    this.render();
    document.getElementById("schemaScreen").addEventListener("click", () => {
      this.checkActiveSession();
    });
  }

  checkActiveSession = () => {
    if (JSON.parse(sessionStorage.getItem("sessionStorage")) != null) {
      window.location.href = "#/schema-report";
    }
  };

  NavLinkTemplate(link) {
    return `
                  <nav class="navbar navbar-static-top">
                    <div class="header-topic">
                      <a name='${link.name}' href="${link.href}" id="${link.aTagId}" class='inactive pointer-style'>
                      ${link.text}
                      </a>
                    </div>
                  </nav>`;
  }

  render() {
    const logoTemplate = `<nav class="${NAVLINKS.logo.css.nav}">
                            <img src="${NAVLINKS.logo.img.src}" class="${NAVLINKS.logo.css.img}">
                          </nav>`;
    this.innerHTML =
      logoTemplate +
      NAVLINKS.links.map((link) => this.NavLinkTemplate(link)).join("");
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-header", Header);
