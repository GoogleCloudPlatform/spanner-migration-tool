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
import { TAB_CONFIG_DATA } from "./../../config/constantData.js";

class Tab extends HTMLElement {
  
  get currentTab() {
    return this.getAttribute("currentTab");
  }

  connectedCallback() {
    this.render(); 
    TAB_CONFIG_DATA.map((tab)=>{
      document.getElementById(tab.id+"Tab").addEventListener('click',()=>{
        Actions.switchCurrentTab(tab.id+"Tab");
      })
    })
  }

  hbTabLink(open,tabId,text) {
    return ` 
      <li class="nav-item">
      <a class="nav-link ${open===true ? "active show" : ""}" id="${tabId}Tab">${text}</a>
      </li>
    `;
  }

  render() {
    let {currentTab} = this;
    this.innerHTML = ` <ul class="nav nav-tabs md-tabs" role="tablist"> ${TAB_CONFIG_DATA.map((tab) => {
      return this.hbTabLink(currentTab===tab.id+"Tab",tab.id,tab.text);
    }).join("")}</ul>`;
  }
  
  constructor() {
    super();
  }
}

window.customElements.define("hb-tab", Tab);
