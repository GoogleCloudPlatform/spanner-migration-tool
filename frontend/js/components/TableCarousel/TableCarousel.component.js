import './../DataTable/DataTable.component.js'
class TableCarousel extends HTMLElement {
  static get observedAttributes() {
    return ["open"];
  }

  get title() {
    return this.getAttribute("title");
  }
  
  get tableClassName(){
      return this.getAttribute('tableClassName')
  }

  get tableId(){
      return this.getAttribute('tableId')
  }

  get tableIndex(){
    return this.getAttribute('tableIndex')
}

  attributeChangedCallback(name, oldValue, newValue) {
    this.render();
  }

  connectedCallback() {
    this.render();
  }

  render() {
    let { title , tableClassName , tableId, tableIndex } = this;  
    this.innerHTML = `
   
                <a data-toggle="collapse" href="#${tableId}">
                    Table: <span>${title}</span>
                    <i class="fas fa-angle-down rotate-icon"></i>
                </a>
                <div class="collapse ${tableClassName}" id="${tableId}">
                  <div class="mdc-card mdc-card-content table-card-border">
                    <hb-data-table tableName="${title}" tableIndex="${tableIndex}"></hb-data-table>
                  </div>
              </div>
           
        
    `;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-table-carousel", TableCarousel);
