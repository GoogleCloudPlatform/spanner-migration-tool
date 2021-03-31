import "./../DataTable/DataTable.component.js";
import "../DdlTable/ListTable.component.js";

class TableCarousel extends HTMLElement {
static get observedAttributes() {
return ["open"];
}

get title() {
return this.getAttribute("title");
}

get tabelId() {
return this.getAttribute("tabelId");
}

attributeChangedCallback(name, oldValue, newValue) {
this.render();
}

connectedCallback() {
this.render();
}

render() {
// let { id, open, text } = this;
let { title, tabelId } = this;

let schemaConversionObj = JSON.parse(
localStorage.getItem("conversionReportContent")
);

let color = JSON.parse(localStorage.getItem("tableBorderColor"));
let colorvalue = color[title];
// console.log(colorvalue);
// console.log(schemaConversionObj);
let carouselContent = schemaConversionObj.SpSchema[title];

this.innerHTML = `
<section class="${tabelId}Section">
  <div class="card" style="border:2px solid ${colorvalue}">
    <div role="tab" class="card-header ${tabelId}-card-header ${tabelId}borderBottom">
      <h5 class="mb-0">
        <a data-toggle="collapse" href="#${tabelId}-${title}">
          Table: <span>${title}</span>
          <i class="fas fa-angle-down rotate-icon"></i>
        </a>
      </h5>
    </div>

    <div class="collapse ${tabelId}Collapse " id="${tabelId}-${title}">
      <div class="mdc-card mdc-card-content table-card-border ${tabelId}-border">
      ${  tabelId == "report" ?
        ` <hb-data-table tableName="${title}"></hb-data-table> `
        :
        ` <hb-list-table tabName="${tabelId}" tableName="${title}"></hb-list-table>`
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