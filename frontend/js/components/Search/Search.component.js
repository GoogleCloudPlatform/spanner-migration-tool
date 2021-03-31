import Actions from "../../services/Action.service.js";
class Search extends HTMLElement {
  get tabId() {
    return this.getAttribute("tabId");
  }

  connectedCallback() {
    this.render();
  }

  render() {
    this.innerHTML = `<i class="fas fa-search" aria-hidden="true"></i>
                        <input class=" w-75 searchBox" type="text" placeholder="Search table"
                        id='searchInput' autocomplete='off' aria-label="Search" >`;

    document
      .getElementById("searchInput")
      .addEventListener("keyup", () =>
        Actions.SearchTable(
          document.getElementById("searchInput").value,
          this.tabId
        )
      );
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-search", Search);
