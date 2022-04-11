// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
