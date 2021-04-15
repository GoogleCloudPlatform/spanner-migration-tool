import Forms from "../../services/Forms.service.js";
import Actions from "../../services/Action.service.js";
import Store from "../../services/Store.service.js";

class AddIndexForm extends HTMLElement {
  get tableName() {
    return this.getAttribute("tableName");
  }

  get tableIndex() {
    return this.getAttribute("tableIndex");
  }

  connectedCallback() {
    const { SrcSchema } = Store.getinstance().tableData.reportTabContent;
    this.render();
    document.getElementById("indexName").addEventListener("focusout", () => {
      Forms.validateInput(
        document.getElementById("indexName"),
        "indexNameError"
      );
    });

    Forms.formButtonHandler("createIndexForm", "createIndexButton");

    document
      .getElementById("createIndexModal")
      .querySelector("i")
      .addEventListener("click", () => {
        Actions.closeSecIndexModal();
      });

    document
      .getElementById("createIndexButton")
      .addEventListener("click", () => {
        Actions.fetchIndexFormValues(
          this.tableIndex,
          this.tableName,
          document.getElementById("indexName").value,
          document.getElementById("uniqueSwitch").checked
        );
      });

    SrcSchema[this.tableName].ColNames.map((row, idx) => {
      document
        .getElementById("checkbox-" + row + "-" + idx)
        .addEventListener("click", () => {
          Actions.changeCheckBox(row, idx);
        });
    });
  }

  render() {
    const { SrcSchema } = Store.getinstance().tableData.reportTabContent;
    this.innerHTML = `
    <form id="createIndexForm">
        <div class="form-group sec-index-label">
            <label for="indexName" class="bmd-label-floating black-label">Enter
                secondary index name</label>
            <input type="text" class="form-control black-border" name="indexName" 
            id="indexName" autocomplete="off">
            <span class='form-error' id='indexNameError'></span>
        </div>
        <div id="newIndexColumnListDiv" class="column-list-container">
              ${SrcSchema[this.tableName].ColNames.map((row, idx) => {
                return `
                <div class="newIndexColumnList" id="indexColumnRow${idx}">
                    <span class="orderId invisible-badge" id="order${row}${idx}">1</span>
                    <span class="columnName">${row}</span>
                    <span class="bmd-form-group is-filled">
                        <div class="checkbox float-right" >
                            <label>
                                <input type="checkbox" value="" id="checkbox-${row}-${idx}">
                                <span class="checkbox-decorator"><span class="check black-border" ></span>
                                    <div class="ripple-container"></div>
                                </span>
                            </label>
                        </div>
                    </span>
                </div>`;
              }).join("")}  
        </div>
        <div class="unique-swith-container">
            <span class="unique-swith-label">Unique</span>
            <label class="switch">
                <input id="uniqueSwitch" type="checkbox">
                <span class="slider round" id="sliderSpan"></span>
            </label>
        </div>
    </form>`;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-add-index-form", AddIndexForm);
