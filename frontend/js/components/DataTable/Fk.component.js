class Fk extends HTMLElement {
  get fkId() {
    return this.getAttribute("fkId");
  }

  get fkArray() {
    return JSON.parse(this.getAttribute("fkArray"));
  }

  attributeChangedCallback(name, oldValue, newValue) {
    this.render();
  }

  connectedCallback() {
    this.render();
  }

  render() {
    const { fkId, fkArray } = this;
    this.innerHTML = `
             <div class="fkCard " style="border-radius: 0px !important">
                        <div class="foreignKeyHeader" role="tab">
                            <h5 class="mb-0">
                                <a class="fkFont" data-toggle="collapse" href="#fk-${fkId}"> Foreign Keys </a>
                            </h5>
                        </div>
                        <div class="collapse fkCollapse" id="fk-${fkId}">
                            <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                <div class="mdc-card fk-content">
                                    <fieldset class="template">
                                        <div class="radio-class">
                                            <input type="radio" class="radio addRadio" value="add" checked="checked"
                                                disabled />
                                            <label style="margin-right: 15px" for="add">
                                                Use as Foreign Key</label>
                                            <input type="radio" class="radio interleaveRadio" value="interleave"
                                                disabled />
                                            <label style="margin-right: 15px" for="interleave">Convert to
                                                Interleave</label>
                                        </div>
                                    </fieldset>
                                   
                                    <table class="fk-acc-table fkTable">
                                        <thead>
                                            <tr>
                                                <th>Name</th>
                                                <th>Columns</th>
                                                <th>Refer Table</th>
                                                <th>Refer Columns</th>
                                                <th>Action</th>
                                            </tr>
                                        </thead>
                                        <tbody class="fkTableBody">
                                        ${fkArray.map((eachFk) => {
                                          return `
                                               <tr class="fkTableTr ">
                                                <td class="acc-table-td fkTableName">
                                                    <div class="renameFk template">
                                                        <input type="text" class="form-control spanner-input"
                                                            autocomplete="off" />
                                                    </div>
                                                    <div class="saveFk">${eachFk.Name}</div>
                                                </td>
                                                <td class="acc-table-td fkTableColumns">${eachFk.Columns[0]}</td>
                                                <td class="acc-table-td fkTableReferTable">${eachFk.ReferTable}</td>
                                                <td class="acc-table-td fkTableReferColumns">${eachFk.ReferColumns[0]}</td>
                                                <td class="acc-table-td fkTableAction">
                                                    <button class="dropButton" data-toggle="tooltip"
                                                        data-placement="bottom"
                                                        title="this will delete foreign key permanently" disabled>
                                                        <span><i class="large material-icons removeIcon"
                                                                style="vertical-align: middle">delete</i></span>
                                                        <span style="vertical-align: middle">Drop</span>
                                                    </button>
                                                </td>
                                            </tr>
                                            `;
                                        }).join("")}





                                            
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
        `;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-data-table-fk", Fk);
