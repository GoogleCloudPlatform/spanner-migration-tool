class ListTable extends HTMLElement {

  get tabName() {
    return this.getAttribute("tabName");
  }

  get tableName() {
    return this.getAttribute("tableName");
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
    let { tabName, tableName } = this;
    let RenderingObj;
    if (tabName === "ddl") {
      RenderingObj = JSON.parse(localStorage.getItem("ddlStatementsContent"));
      RenderingObj = RenderingObj[tableName];
      RenderingObj = this.FormattedObj(RenderingObj);
    } else {
      RenderingObj = JSON.parse(localStorage.getItem("summaryReportContent"));
      RenderingObj = RenderingObj[tableName];
    }

    this.innerHTML = `
        <div class='mdc-card ${tabName}-content'>
        ${tabName == "ddl" ? `<pre> <code>` : `<div>`}
           ${RenderingObj?.split("\n").join(`<span class='sql-c'></span>`)}
        ${tabName == "ddl" ? `</code> </pre>` : `</div>`}
        </div>
        `;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-list-table", ListTable);
