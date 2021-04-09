const navLinks = {
  logo: {
    css: { nav: "navbar navbar-static-top", img: "logo" },
    img: { src: "../../../Icons/Icons/google-spanner-logo.png" },
  },
  links: [
    {
      text: "Home",
      href: "#/",
      aTagId: "homeScreen",
      name: "headerMenu",
    },
    {
      text: "Schema Conversion",
      href: "javascript:;",
      aTagId: "schemaScreen",
      name: "headerMenu",
    },
    {
      text: "Instructions",
      href: "#/instructions",
      aTagId: "instructions",
      name: "headerMenu",
    },
  ],
};

class Header extends HTMLElement {
  connectedCallback() {
    this.render();
    document.getElementById("schemaScreen").addEventListener("click", () => {
      this.checkActiveSession();
    });
  }

  checkActiveSession = () => {
    console.log("comming inside");
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
    const logoTemplate = `<nav class="${navLinks.logo.css.nav}">
                            <img src="${navLinks.logo.img.src}" class="${navLinks.logo.css.img}">
                          </nav>`;
    this.innerHTML =
      logoTemplate +
      navLinks.links.map((link) => this.NavLinkTemplate(link)).join("");
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-header", Header);
