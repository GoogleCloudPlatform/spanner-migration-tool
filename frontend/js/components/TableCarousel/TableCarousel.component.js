import './../DataTable/DataTable.component.js'
class TableCarousel extends HTMLElement {
  static get observedAttributes() {
    return ["open"];
  }

  get title() {
    return this.getAttribute("title");
  }
  
  get tabelClassName(){
      return this.getAttribute('tabelClassName')
  }

  get tabelId(){
      return this.getAttribute('tabelId')
  }

  attributeChangedCallback(name, oldValue, newValue) {
    this.render();
  }

  connectedCallback() {
    this.render();
  }

  render() {
    // let { id, open, text } = this;
    let { title , tabelClassName , tabelId } = this;
  
    let schemaConversionObj = JSON.parse(
      localStorage.getItem("conversionReportContent")
    );
    // console.log(schemaConversionObj);
    let carouselContent = schemaConversionObj.SpSchema[title];
  
    this.innerHTML = `
   
                <a data-toggle="collapse" href="#${tabelId}">
                    Table: <span>${title}</span>
                    <i class="fas fa-angle-down rotate-icon"></i>
                </a>
                <div class="collapse ${tabelClassName}" id="${tabelId}">
                  <div class="mdc-card mdc-card-content table-card-border">
                    <hb-data-table></hb-data-table>
                  </div>
              </div>
           
        
    `;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-table-carousel", TableCarousel);
