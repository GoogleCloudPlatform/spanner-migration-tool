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

class ListTable extends HTMLElement {

  get tabName() {
    return this.getAttribute("tabName");
  }

  get tableName() {
    return this.getAttribute("tableName");
  }

  get data() {
    return this.getAttribute("dta");
  }

  static get observedAttributes() {
    return ["open"];
  }

  attributeChangedCallback(name, oldValue, newValue) {
    this.render();
  }

  connectedCallback() {
    this.render();
  }

  FormattedObj(RenderingObj) {
    let createIndex = RenderingObj.search("CREATE TABLE");
    let createEndIndex = createIndex + 12;
    RenderingObj =
      RenderingObj.substring(0, createIndex) +
      RenderingObj.substring(createIndex, createEndIndex)
                  .fontcolor("#4285f4")
                  .bold() +
      RenderingObj.substring(createEndIndex);
    return RenderingObj;
  }

  render() {
    let { tabName, data } = this;
    let ddlStr = data;
    ddlStr = ddlStr.replaceAll("<","&lt;").replaceAll(">","&gt;")
    if (tabName === "ddl") {
      ddlStr = this.FormattedObj(ddlStr);
    } 
    this.innerHTML = `
        <div class='mdc-card ${tabName}-content'>
        ${tabName == "ddl" ?`<pre><code>` : `<div>`}${ddlStr?.split("\n").
        join(`<span class='sql-c'></span>`)}${tabName == "ddl" ?`</code> </pre>`:`</div>`}</div>`;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-list-table", ListTable);
