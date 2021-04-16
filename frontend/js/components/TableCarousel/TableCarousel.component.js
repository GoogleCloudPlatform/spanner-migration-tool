import Actions from "../../services/Action.service.js";
import Store from "../../services/Store.service.js";
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

  get tableTitle() {
    return this.getAttribute("tableTitle");
  }

  get tableId() {
    return this.getAttribute("tableId");
  }

  get tableIndex() {
    return this.getAttribute("tableIndex");
  }

  get data() {
    return this._data;
  }

  set data(value){
    this._data = value;
    this.render();
    this.addEventListenertoCarausal();
    document.querySelector(`hb-data-table[tableName=${this.tableTitle}`).data =this._data; 
    console.log(document.querySelector(`hb-data-table[tableName=${this.tableTitle}`));
  }

  get borderData() {
    return this.getAttribute("borderData");
  }

  // static get observedAttributes() {
  //   return ["data"];
  // }

  addEventListenertoCarausal() {
    document.getElementById(`id-${this.tableId}-${this.tableIndex}`).addEventListener('click',()=>{
      if(Store.getinstance().openStatus[this.tableId][this.tableIndex])
      {
        Actions.closeCarousel(this.tableId , this.tableIndex)
      }
      else{
        Actions.openCarousel(this.tableId , this.tableIndex)
      }
    })
  }

  // attributeChangedCallback(attrName, oldVal, newVal ) {
  //     //  console.log(oldVal,newVal);
  //   if (attrName === 'data' && newVal !== "{}" && oldVal!==null) {
  //       this.render();
  //       if(newVal!=="{}" && oldVal=="{}") {
  //         this.addEventListenertoCarausal();
  //       }
  //     }
  // }

  connectedCallback() {
    if(this.tableId!="report")
    {
      this.render();
      this.addEventListenertoCarausal();
    } 
  }

  render() {
  //  console.log(this.data);
    let {tableTitle, tableId, tableIndex, data, borderData} = this;
    // console.log(data);
    if(tableId == "report" && data == "{}"){
      console.log("inside");
      return ;
    }
    let color = borderData;
    let panelColor = panelBorderClass(color);
    let cardColor = mdcCardBorder(color);
    let carouselStatus = Store.getinstance().openStatus[this.tableId][this.tableIndex];
    let editButtonVisibleClass = carouselStatus ? 'show-content' : 'hide-content'
    this.innerHTML = `
    <section class="${tableId}Section" id="${tableIndex}">
      <div class="card">

        <div role="tab" class="card-header ${tableId}-card-header ${panelColor} rem-border-bottom">
          <h5 class="mb-0">
            <a data-toggle="collapse" id="id-${tableId}-${tableIndex}">
              Table: <span>${tableTitle}</span>
              <i class="fas fa-angle-${carouselStatus?'up':'down'} rotate-icon"></i>
            </a>
            ${ tableId ==="report" ? `
                <span class="spanner-text right-align ${editButtonVisibleClass}">Spanner</span>
                <span class="spanner-icon right-align ${editButtonVisibleClass}">
                  <i class="large material-icons iconSize">circle</i>
                </span>
                <span class="source-text right-align ${editButtonVisibleClass}">Source</span>
                <span class="source-icon right-align ${editButtonVisibleClass}">
                  <i class="large material-icons iconSize">circle</i>
                </span>
                <button class="right-align edit-button ${editButtonVisibleClass}" id="editSpanner${tableIndex}">
                  Edit Spanner Schema
                </button>
                <span id="editInstruction${tableIndex}" class="right-align editInstruction ${editButtonVisibleClass} blink">
                  Schema locked for editing. Unlock to change =>
                </span> `
                :
                ` <div></div> `
             }
          </h5>
        </div>
    
        <div class="collapse ${tableId}Collapse ${carouselStatus?"show bs collapse":""}" id="${tableId}-${tableTitle}">
          <div class="mdc-card mdc-card-content table-card-border ${cardColor}">
            ${ tableId == "report" ? `
            <hb-data-table tableName="${tableTitle}" tableIndex="${tableIndex}" ></hb-data-table>` 
            :
            `<hb-list-table tabName="${tableId}" tableName="${tableTitle}" dta="${data}"></hb-list-table>`
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
