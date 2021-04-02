import Actions from "../../services/Action.service.js";
class Search extends HTMLElement {
  get tabId() {
    return this.getAttribute("tabid");
  }

  get searchId() {
    return this.getAttribute("searchid");
  }

  connectedCallback() {
    this.render();
  }

  render() {
const {searchId,tabId} = this;
    this.innerHTML= `
    <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 searchForm" >
            <i class="fas fa-search" aria-hidden="true"></i>
            <input class="form-control form-control-sm ml-3 w-75 searchBox" type="text" placeholder="Search table"
                id=${searchId} autocomplete='off' aria-label="Search" >
        </form>`
   

    document
      .getElementById(this.searchId)
      .addEventListener("keyup", () =>
        Actions.SearchTable(
          document.getElementById(this.searchId).value,
          this.tabId
        )
      );
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-search", Search);
