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
