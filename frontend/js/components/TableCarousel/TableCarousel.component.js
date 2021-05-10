import Actions from "../../services/Action.service.js";
import "../DataTable/DataTable.component.js";
import "../ListTable/ListTable.component.js";
import {
  panelBorderClass,
  mdcCardBorder,
} from "./../../helpers/SchemaConversionHelper.js";

class TableCarousel extends HTMLElement {

  get tableTitle() {
    return this.getAttribute("tableTitle");
  }

  get tabId() {
    return this.getAttribute("tabId");
  }

  get tableIndex() {
    return this.getAttribute("tableIndex");
  }

  get data() {
    return this._data;
  }

  get stringData() {
     return this.getAttribute("stringData");
  }

  set data(value){
    this._data = value;
    this.render();
    this.addEventListenertoCarausal();
    if(Actions.carouselStatus(this.tabId)[this.tableIndex]){
      document.querySelector(`hb-data-table[tableName=${this.tableTitle}`).data =this._data; 
    }
   
  }

  get borderData() {
    return this.getAttribute("borderData");
  }

  addEventListenertoCarausal() {
    document.getElementById(`id-${this.tabId}-${this.tableIndex}`).addEventListener('click',()=>{
      Actions.showSpinner()
      if(Actions.carouselStatus(this.tabId)[this.tableIndex])
      {
        Actions.closeCarousel(this.tabId , this.tableIndex)
      }
      else{
        Actions.openCarousel(this.tabId , this.tableIndex)
      }
    })
  }

  connectedCallback() {
    if(this.tabId!="report")
    {
      this.render();
      this.addEventListenertoCarausal();
    } 
  }

  render() {
    let {tableTitle, tabId, tableIndex, data, borderData, stringData} = this;
    let color = borderData;
    let panelColor = panelBorderClass(color);
    let cardColor = mdcCardBorder(color);
    let carouselStatus = Actions.carouselStatus(this.tabId)[this.tableIndex];
    let editButtonVisibleClass = carouselStatus ? 'show-content' : 'hide-content';
    let tableMode = Actions.getTableMode(tableIndex);

 
    this.innerHTML = `
    <section class="${tabId}-section" id="${tableIndex}">
      <div class="card">
        <div role="tab" class="card-header ${tabId}-card-header ${carouselStatus ? 'no-border-bottom  rem-border-bottom':''}  ${panelColor}">
          <h5 class="mb-0">
            <a data-toggle="collapse" id="id-${tabId}-${tableIndex}">
              Table: <span>${tableTitle}</span>
              <i class="rotate-icon fas fa-angle-${carouselStatus?'up':'down'} "></i>
            </a>
            ${ tabId ==="report" ? `
                <span class="spanner-text right-align ${editButtonVisibleClass}">Spanner</span>
                <span class="spanner-icon right-align ${editButtonVisibleClass}">
                  <i class="large material-icons round-icon-size">circle</i>
                </span>
                <span class="source-text right-align ${editButtonVisibleClass}">Source</span>
                <span class="source-icon right-align ${editButtonVisibleClass}">
                  <i class="large material-icons round-icon-size">circle</i>
                </span>
                <button class="edit-button ${editButtonVisibleClass}" id="editSpanner${tableIndex}">
                  ${tableMode?"Save Changes" : " Edit Spanner Schema"}
                </button>
                ${tableMode==false && carouselStatus ? `
                  <span id="edit-instruction${tableIndex}" class="right-align edit-instruction blink ">
                  Schema locked for editing. Unlock to change =>
                </span> `:`<div></div>`}`
                :
                ` <div></div> `
             }
          </h5>
        </div>
         ${
           carouselStatus ? `<div class="collapse ${tabId}-collapse show bs collapse show-carausel" id="${tabId}-${tableTitle}">
           <div class="mdc-card mdc-card-content table-card-border ${cardColor}">
             ${ tabId == "report" ? `
             <hb-data-table tableName="${tableTitle}" tableIndex="${tableIndex}" ></hb-data-table>` 
             :
             `<hb-list-table tabName="${tabId}" tableName="${tableTitle}" dta="${stringData}"></hb-list-table>`
            }
           </div>
         </div>`:`<div></div>`
         }
        

      </div>
    </section> `;
   
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-table-carousel", TableCarousel);
