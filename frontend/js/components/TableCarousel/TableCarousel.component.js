import "../DataTable/DataTable.component.js";
import "../ListTable/ListTable.component.js";
import {panelBorderClass} from './../../helpers/SchemaConversionHelper.js';
import Actions from './../../services/Action.service.js';
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
let colorvalue = panelBorderClass(color[title]);

this.innerHTML = `
<section class="${tableId}Section">
  <div class="card ${colorvalue}">
    <div role="tab" class="card-header ${tableId}-card-header ${tableId}borderBottom">
      <h5 class="mb-0">
        <a data-toggle="collapse" href="#${tableId}-${title}">
          Table: <span>${title}</span>
          <i class="fas fa-angle-down rotate-icon"></i>
          ${ tableId == "report" ?
          ` 
          <span class='spanner-text right-align hide-content'>Spanner</span>
                                        <span class='spanner-icon right-align hide-content'>
                                            <i class='large material-icons' style='font-size: 18px;'>circle</i>
                                        </span>
                                        <span class='source-text right-align hide-content'>Source</span>
                                        <span class='source-icon right-align hide-content'>
                                            <i class='large material-icons' style='font-size: 18px;'>circle</i>
                                        </span>
                                        <button class='right-align edit-button hide-content'>
                                            Edit Spanner Schema
                                        </button>
                                        <span class='right-align editInstruction hide-content blink'>Schema locked for
                                            editing. Unlock to change =></span>
          ` : 
          `<div></div>`
          }
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
