import "../DataTable/DataTable.component.js";
import "../ListTable/ListTable.component.js";
import {
  panelBorderClass,
  mdcCardBorder,
} from "./../../helpers/SchemaConversionHelper.js";

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
    let { title, tableId, tableIndex } = this;
    let color = JSON.parse(localStorage.getItem("tableBorderColor"));
    let panelColor = panelBorderClass(color[title]);
    let cardColor = mdcCardBorder(color[title]);

    this.innerHTML = `
    <section class="${tableId}Section" id="${tableIndex}">
      <div class="card">

        <div role="tab" class="card-header ${tableId}-card-header ${panelColor} rem-border-bottom">
          <h5 class="mb-0">
            <a data-toggle="collapse" href="#${tableId}-${title}">
              Table: <span>${title}</span>
              <i class="fas fa-angle-down rotate-icon"></i>
            </a>
            ${ tableId == "report" ? `
                <span class="spanner-text right-align hide-content">Spanner</span>
                <span class="spanner-icon right-align hide-content">
                  <i class="large material-icons iconSize">circle</i>
                </span>
                <span class="source-text right-align hide-content">Source</span>
                <span class="source-icon right-align hide-content">
                  <i class="large material-icons iconSize">circle</i>
                </span>
                <button class="right-align edit-button hide-content" id="editSpanner${tableIndex}">
                  Edit Spanner Schema
                </button>
                <span id="editInstruction${tableIndex}" class="right-align editInstruction hide-content blink">
                  Schema locked for editing. Unlock to change =>
                </span> `
                :
                ` <div></div> `
             }
          </h5>
        </div>
    
        <div class="collapse ${tableId}Collapse" id="${tableId}-${title}">
          <div class="mdc-card mdc-card-content table-card-border ${cardColor}">
            ${ tableId == "report" ? `
            <hb-data-table tableName="${title}" tableIndex="${tableIndex}"></hb-data-table>` 
            :
            `<hb-list-table tabName="${tableId}" tableName="${title}"></hb-list-table>`
           }
          </div>
        </div>

      </div>
    </section> `;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-table-carousel", TableCarousel);
