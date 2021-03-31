import "../DataTable/DataTable.component.js";
import "../ListTable/ListTable.component.js";

class TableCarousel extends HTMLElement {
static get observedAttributes() {
return ["open"];
}

get title() {
return this.getAttribute("title");
}

get tableId() {
return this.getAttribute("tableId");
}

get tableIndex() {
  return this.getAttribute("tableIndex");
  }

attributeChangedCallback(name, oldValue, newValue) {
this.render();
}

connectedCallback() {
this.render();
}

render() {
// let { id, open, text } = this;
let { title, tableId, tableIndex } = this;
let color = JSON.parse(localStorage.getItem("tableBorderColor"));
let colorvalue = color[title];


this.innerHTML = `
<section class="${tableId}Section">
  <div class="card" style="border:2px solid ${colorvalue}">
    <div role="tab" class="card-header ${tableId}-card-header ${tableId}borderBottom">
      <h5 class="mb-0">
        <a data-toggle="collapse" href="#${tableId}-${title}">
          Table: <span>${title}</span>
          <i class="fas fa-angle-down rotate-icon"></i>
        </a>
      </h5>
    </div>

    <div class="collapse ${tableId}Collapse " id="${tableId}-${title}">
      <div class="mdc-card mdc-card-content table-card-border ${tableId}-border">
      ${  tableId == "report" ?
        ` <hb-data-table tableName="${title}" tableIndex="${tableIndex}"></hb-data-table> `
        :
        ` <hb-list-table tabName="${tableId}" tableName="${title}"></hb-list-table>`
        }
      </div>
    </div>
  </div>
</section>
`;
}

constructor() {
super();
}

}

window.customElements.define("hb-table-carousel", TableCarousel);
