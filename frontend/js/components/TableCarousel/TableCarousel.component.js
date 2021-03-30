import './../DataTable/DataTable.component.js'
class TableCarousel extends HTMLElement {
  static get observedAttributes() {
    return ["open"];
  }

  get title() {
    return this.getAttribute("title");
  }
  
  get className(){
      return this.getAttribute('className')
  }

  get id(){
      return this.getAttribute('id')
  }

  attributeChangedCallback(name, oldValue, newValue) {
    this.render();
  }

  connectedCallback() {
    this.render();
  }

  render() {
    // let { id, open, text } = this;
    let { title , className , id } = this;
  
    let schemaConversionObj = JSON.parse(
      localStorage.getItem("conversionReportContent")
    );
    // console.log(schemaConversionObj);
    let carouselContent = schemaConversionObj.SpSchema[title];
  
    this.innerHTML = `
    <section class="reportSection">
    <div class="card">
        <div role="tab" class="card-header report-card-header borderBottom">
            <h5 class="mb-0">
                <a data-toggle="collapse" href="#${id}">
                    Table: <span>${title}</span>
                    <i class="fas fa-angle-down rotate-icon"></i>
                </a>
            </h5>
        </div>
        <div class="collapse ${className}" id="${id}">
            <div class="mdc-card mdc-card-content table-card-border">
                <hb-data-table></hb-data-table>
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
