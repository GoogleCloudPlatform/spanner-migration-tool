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

  add(a,b){
   return Actions.add(a,b)
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

        case "add":
          Actions[this.buttonAction](5,6)
          break;
          
        default:
          if(Actions[this.buttonAction])
          {
            Actions[this.buttonAction]();
          }
          break;
      }
    });
  }
}

window.customElements.define("hb-site-button", SiteButton);
