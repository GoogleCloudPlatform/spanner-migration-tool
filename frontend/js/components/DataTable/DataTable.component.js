import Actions from "../../services/Action.service.js";

class DataTable extends HTMLElement {
    static get observedAttributes() {
        return ["open"];
    }

    get id() {
        return this.getAttribute("id");
    }

    attributeChangedCallback(name, oldValue, newValue) {
        this.render();
    }

    connectedCallback() {
        this.render(); 
    }

    render() {
        let { id, open, text } = this;
        this.innerHTML = `
        <div class="acc-card-content" id="acc_card_content">
                    <table class="acc-table">
                        <thead>
                            <tr>
                                <th class="acc-column" colspan="2">Column Name</th>
                                <th class="acc-column" colspan="2">Data Type</th>
                                <th class="acc-column" colspan="2">Constraints</th>
                            </tr>
                            <tr>
                                <th class="acc-table-th-src src-tab-cell">
                                    <span class="bmd-form-group is-filled template">
                                        <div class="checkbox">
                                            <label>
                                                <input type="checkbox" value="" />
                                                <span class="checkbox-decorator"><span class="check"
                                                        style="margin-left: -7px"></span>
                                                    <div class="ripple-container"></div>
                                                </span>
                                            </label>
                                        </div>
                                    </span>
                                </th>
                                <th class="acc-table-th-spn">Spanner</th>
                                <th class="acc-table-th-src"></th>
                                <th class="acc-table-th-spn">Spanner</th>
                                <th class="acc-table-th-src"></th>
                                <th class="acc-table-th-spn">Spanner</th>
                            </tr>
                        </thead>
                        <tbody class="acc-table-body">
                            <tr class="reportTableContent template">
                                <td class="acc-table-td src-tab-cell">
                                    <span class="bmd-form-group is-filled eachRowChckBox template">
                                        <div class="checkbox">
                                            <label>
                                                <input type="checkbox" value="" />
                                                <span class="checkbox-decorator"><span class="check"></span>
                                                    <div class="ripple-container"></div>
                                                </span>
                                            </label>
                                        </div>
                                    </span>
                                    <span class="column left">
                                        <img class="srcPk" src="./Icons/Icons/ic_vpn_key_24px.svg"
                                            style="margin-left: 3px" />
                                    </span>
                                    <span class="column right srcColumn"></span>
                                </td>
                                <td class="sp-column acc-table-td spannerColName">
                                    <div class="editColumnName template">
                                        <span class="column left keyMargin keyClick">
                                            <sub></sub><img />
                                        </span>
                                        <span class="column right form-group">
                                            <input type="text" class="form-control spanner-input" autocomplete="off" />
                                        </span>
                                    </div>
                                    <div class="saveColumnName template">
                                        <span class="column left spannerPkSpan">
                                            <sub></sub>
                                            <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey" />
                                        </span>
                                        <span class="column right spannerColNameSpan"></span>
                                    </div>
                                </td>
                                <td class="acc-table-td srcDataType"></td>
                                <td class="sp-column acc-table-td spannerDataType">
                                    <div class="saveDataType template"></div>
                                    <div class="editDataType template">
                                        <div class="form-group">
                                            <select class="form-control spanner-input tableSelect">
                                                <option class="dataTypeOption template"></option>
                                            </select>
                                        </div>
                                    </div>
                                </td>
                                <td class="acc-table-td">
                                    <select multiple size="1"
                                        class="form-control spanner-input tableSelect srcConstraint">
                                        <option disabled class="srcNotNullConstraint">
                                            Not Null
                                        </option>
                                    </select>
                                </td>
                                <td class="acc-table-td sp-column acc-table-td">
                                    <div class="saveConstraint template">
                                        <select multiple size="1"
                                            class="form-control spanner-input tableSelect spannerConstraint">
                                            <option disabled class="spannerNotNullConstraint">
                                                Not Null
                                            </option>
                                        </select>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                    <div class="fkCard template" style="border-radius: 0px !important">
                        <div class="foreignKeyHeader" role="tab">
                            <h5 class="mb-0">
                                <a class="fkFont" data-toggle="collapse"> Foreign Keys </a>
                            </h5>
                        </div>
                        <div class="collapse fkCollapse">
                            <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                <div class="mdc-card fk-content">
                                    <fieldset>
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
                                    <br />
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
                                            <tr class="fkTableTr template">
                                                <td class="acc-table-td fkTableName">
                                                    <div class="renameFk template">
                                                        <input type="text" class="form-control spanner-input"
                                                            autocomplete="off" />
                                                    </div>
                                                    <div class="saveFk template"></div>
                                                </td>
                                                <td class="acc-table-td fkTableColumns"></td>
                                                <td class="acc-table-td fkTableReferTable"></td>
                                                <td class="acc-table-td fkTableReferColumns"></td>
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
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="indexesCard template" style="border-radius: 0px !important">
                        <div class="foreignKeyHeader" role="tab">
                            <h5 class="mb-0">
                                <a class="indexFont" data-toggle="collapse">
                                    Secondary Indexes
                                </a>
                            </h5>
                        </div>
                        <div class="collapse indexCollapse">
                            <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                <div class="mdc-card fk-content">
                                    <button class="newIndexButton" onclick="createNewSecIndex(this.id)">
                                        Add Index
                                    </button>
                                    <table class="index-acc-table fkTable">
                                        <thead>
                                            <tr>
                                                <th>Name</th>
                                                <th>Table</th>
                                                <th>Unique</th>
                                                <th>Keys</th>
                                                <th>Action</th>
                                            </tr>
                                        </thead>
                                        <tbody class="indexTableBody">
                                            <tr class="indexTableTr template">
                                                <td class="acc-table-td indexesName">
                                                    <div class="renameSecIndex template">
                                                        <input type="text" class="form-control spanner-input"
                                                            autocomplete="off" />
                                                    </div>
                                                    <div class="saveSecIndex template"></div>
                                                </td>
                                                <td class="acc-table-td indexesTable"></td>
                                                <td class="acc-table-td indexesUnique"></td>
                                                <td class="acc-table-td indexesKeys"></td>
                                                <td class="acc-table-td indexesAction">
                                                    <button class="dropButton" disabled>
                                                        <span><i class="large material-icons removeIcon"
                                                                style="vertical-align: middle">delete</i></span>
                                                        <span style="vertical-align: middle">Drop</span>
                                                    </button>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="summaryCard">
                        <div class="summaryCardHeader" role="tab">
                            <h5 class="mb-0">
                                <a data-toggle="collapse" class="summaryFont">View Summary</a>
                            </h5>
                        </div>
                        <div class="collapse innerSummaryCollapse">
                            <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                <div class="mdc-card summary-content"></div>
                            </div>
                        </div>
                    </div>
                </div>`;
    }

    constructor() {
        super();
        this.addEventListener("click", () => Actions.switchToTab(this.id));
    }
}

window.customElements.define("hb-data-table", DataTable);
