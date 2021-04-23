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
    let RenderingObj = data;
    if (tabName === "ddl") {
      RenderingObj = this.FormattedObj(RenderingObj);
    } 
    this.innerHTML = `
        <div class='mdc-card ${tabName}-content'>
        ${tabName == "ddl" ?`<pre><code>` : `<div>`}${RenderingObj?.split("\n").
        join(`<span class='sql-c'></span>`)}${tabName == "ddl" ?`</code> </pre>`:`</div>`}</div>`;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-list-table", ListTable);
