import "./../Modal/Modal.component.js";
import { recreateNode } from './../../helpers/SchemaConversionHelper.js';
import Actions from './../../services/Action.service.js';
import Store from './../../services/Store.service.js';

class DataTable extends HTMLElement {

    get tableName() {
        return this.getAttribute("tableName");
    }

    get tableIndex() {
        return this.getAttribute("tableIndex");
    }

    get data() {
        return this._dta
    }

    set data(value){
        this._dta = value;
        this.render()
    }

    connectedCallback() {
        if(Store.getinstance().checkInterleave[this.tableName] === undefined)
        {
            Actions.checkInterleaveConversion(this.tableName);
        }
        this.checkInterLeave = Store.getinstance().checkInterleave[this.tableName]; 
     }   

    fkComponent(tableIndex, tableName, fkArray) {
        return `
            <div class="fkCard">
                <div class="foreignKeyHeader" role="tab">
                    <h5 class="mb-0">
                        <a class="fkFont" data-toggle="collapse" href="#foreignKey${tableIndex}"> Foreign Keys </a>
                    </h5>
                </div>
                <div class="collapse fkCollapse" id="foreignKey${tableIndex}">
                    <div class="mdc-card mdc-card-content summaryBorder">
                        <div class="mdc-card fk-content">
                            <fieldset class=${this.checkInterLeave == true ? "" : "template"} id="radioBtnArea${tableIndex}">
                                <div class="radio-class">
                                    <input type="radio" class="radio addRadio" value="add" checked="checked" disabled
                                        id="add${tableIndex}" name="fks${tableIndex}" />
                                    <label for="add">
                                        Use as Foreign Key</label>
                                    <input type="radio" class="radio interleaveRadio" value="interleave" disabled
                                        id="interleave${tableIndex}" name="fks${tableIndex}" />
                                    <label for="interleave">Convert to
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
                                <tbody class="fkTableBody" id="fkTableBody${tableIndex}">
                                    ${fkArray.map((fk, index) => {
                                    return `
                                    <tr class="fkTableTr ">
                                        <td class="acc-table-td fkTableName">
                                            <div class="renameFk template" id="renameFk${tableIndex}${index}">
                                                <input type="text" class="form-control spanner-input" autocomplete="off"
                                                    id="newFkVal${tableIndex}${index}" value=${fk.Name} />
                                            </div>
                                            <div class="saveFk" id="saveFk${tableIndex}${index}">${fk.Name}</div>
                                        </td>
                                        <td class="acc-table-td fkTableColumns">${fk.Columns[0]}</td>
                                        <td class="acc-table-td fkTableReferTable">${fk.ReferTable}</td>
                                        <td class="acc-table-td fkTableReferColumns">${fk.ReferColumns[0]}</td>
                                        <td class="acc-table-td fkTableAction">
                                            <button class="dropButton" id="${tableName}${index}foreignKey" data-toggle="tooltip"
                                                data-placement="bottom" title="this will delete foreign key permanently" disabled>
                                                <span><i class="large material-icons removeIcon verticalAlignmentMiddle">delete</i></span>
                                                <span class="verticalAlignmentMiddle">Drop</span>
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

    secIndexComponent(tableIndex, tableName, secIndexArray) {
        return `
            <div class="indexesCard">
                <div class="foreignKeyHeader" role="tab">
                    <h5 class="mb-0">
                        <a class="indexFont" data-toggle="collapse" href="#indexKey${tableIndex}">
                            Secondary Indexes
                        </a>
                    </h5>
                </div>
                <div class="collapse indexCollapse" id="indexKey${tableIndex}">
                    <div class="mdc-card mdc-card-content summaryBorder">
                        <div class="mdc-card fk-content">
                            <hb-site-button buttonid="${tableIndex}indexButton-${this.tableName}" classname="newIndexButton"
                                buttonaction="createNewSecIndex" text="Add Index"></hb-site-button>

                            <table class="index-acc-table fkTable">
                                ${ secIndexArray && secIndexArray.length > 0 ? `<thead>
                                    <tr>
                                        <th>Name</th>
                                        <th>Table</th>
                                        <th>Unique</th>
                                        <th>Keys</th>
                                        <th>Action</th>
                                    </tr>
                                </thead>`: `<div></div>`}
                                <tbody class="indexTableBody" id="indexTableBody${tableIndex}">
                                    ${secIndexArray ? secIndexArray?.map((secIndex, index) => {
                                    return `
                                    <tr class="indexTableTr ">
                                        <td class="acc-table-td indexesName">
                                            <div class="renameSecIndex template" id="renameSecIndex${tableIndex}${index}">
                                                <input type="text" id="newSecIndexVal${tableIndex}${index}" value=${secIndex.Name}
                                                    class="form-control spanner-input" autocomplete="off" />
                                            </div>
                                            <div class="saveSecIndex" id="saveSecIndex${tableIndex}${index}">${secIndex.Name}</div>
                                        </td>
                                        <td class="acc-table-td indexesTable">${secIndex.Table}</td>
                                        <td class="acc-table-td indexesUnique">${secIndex.Unique}</td>
                                        <td class="acc-table-td indexesKeys">${secIndex?.Keys.map((key) => key.Col).join(',')}</td>
                                        <td class="acc-table-td indexesAction">
                                            <button class="dropButton" id="${tableName}${index}secIndex" data-toggle="tooltip"
                                                data-placement="bottom" title="this will delete secondary index permanently"
                                                disabled>
                                                <span><i class="large material-icons removeIcon verticalAlignmentMiddle">delete</i></span>
                                                <span class="verticalAlignmentMiddle">Drop</span>
                                            </button>
                                        </td>
                                    </tr>
                                    `;
                                    }).join("") : ``
                                }
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    render() {
        let { tableName, tableIndex, data } = this;
        let countSrc = [], countSp = [], notNullConstraint = [];
        let spTable = data.SpSchema;
        let srcTable = data.SrcSchema;
        let tableColumnsArray = data.SpSchema.ColNames;
        let pksSp = [...spTable.Pks];
        let pksSpLength = pksSp.length;
        let pkSeqId = 1;
        countSrc[tableIndex] = [];
        countSp[tableIndex] = [];
        for (var x = 0; x < pksSpLength; x++) { if (pksSp[x].seqId == undefined) { pksSp[x].seqId = pkSeqId; pkSeqId++; } }
        let sourceDbName = Store.getSourceDbName()
        this.innerHTML = ` <div class="acc-card-content" id="acc_card_content">
                                <table class="acc-table" id="src-sp-table${tableIndex}">
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
                                                            <input type="checkbox" value="" id="chckAll_${tableIndex}" />
                                                            <span class="checkbox-decorator"><span class="check ml7"></span>
                                                                <div class="ripple-container"></div>
                                                            </span>
                                                        </label>
                                                    </div>
                                                </span>
                                                ${sourceDbName}
                                            </th>
                                            <th class="acc-table-th-spn">Spanner</th>
                                            <th class="acc-table-th-src">${sourceDbName}</th>
                                            <th class="acc-table-th-spn">Spanner</th>
                                            <th class="acc-table-th-src">${sourceDbName}</th>
                                            <th class="acc-table-th-spn">Spanner</th>
                                        </tr>
                                    </thead>
                                    <tbody class="acc-table-body">
                                        ${tableColumnsArray.map((tableColumn, index) => {
                                        let pkFlag = false, seqId;
                                        countSrc[tableIndex][index] = 0;
                                        countSp[tableIndex][index] = 0;
                                        for (var x = 0; x < pksSpLength; x++) {
                                            if (pksSp[x].Col === tableColumn) {
                                                pkFlag = true; seqId = pksSp[x].seqId;
                                                break
                                            }
                                        } 
                                        let currentColumnSrc = data.ToSource.Cols[tableColumn]; return `
                                            <tr class="reportTableContent">
                                            <td class="acc-table-td src-tab-cell">
                                                <span class="bmd-form-group is-filled eachRowChckBox template">
                                                    <div class="checkbox">
                                                        <label>
                                                            <input type="checkbox" value="" id="chckBox_${tableIndex}"
                                                                class="chckClass_${tableIndex}" />
                                                            <span class="checkbox-decorator"><span class="check"></span>
                                                                <div class="ripple-container"></div>
                                                            </span>
                                                        </label>
                                                    </div>
                                                </span>
                                                <span class="column left">
                                                    ${(currentColumnSrc != srcTable.PrimaryKeys[0].Column || srcTable.PrimaryKeys === null) ?
                                                `<img class="srcPk hidden ml-3" src="./Icons/Icons/ic_vpn_key_24px.svg" />` :
                                                `<img class="srcPk ml-3" src="./Icons/Icons/ic_vpn_key_24px.svg" />`}

                                                </span>
                                                <span class="column right srcColumn"
                                                    id="srcColumnName${tableIndex}${index}${index}">${currentColumnSrc}</span>
                                            </td>
                                            <td class="sp-column acc-table-td spannerColName spannerTabCell${tableIndex}${index}">
                                                <div class="editColumnName template" id="editColumnName${tableIndex}${index}">
                                                    <span class="column left keyMargin keyClick">
                                                        ${pkFlag ?
                                                `<sub>${seqId}</sub>
                                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey" />` :
                                                `<sub></sub>
                                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey hidden" />`}
                                                    </span>
                                                    <span class="column right form-group">
                                                        <input type="text" class="spanner-input form-control"
                                                            id="columnNameText${tableIndex}${index}${index}" autocomplete="off"
                                                            value=${tableColumn} />
                                                    </span>
                                                </div>
                                                <div class="saveColumnName" id="saveColumnName${tableIndex}${index}">
                                                    <span class="column left spannerPkSpan pointer">
                                                        ${pkFlag ?
                                                `<sub>${seqId}</sub>
                                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey" />` :
                                                `<sub></sub>
                                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey hidden" />`}

                                                    </span>
                                                    <span class="column right spannerColNameSpan pointer">${tableColumn}</span>
                                                </div>
                                            </td>
                                            <td class="acc-table-td srcDataType" id="srcDataType${tableIndex}${index}">
                                                ${srcTable.ColDefs[currentColumnSrc].Type.Name}</td>
                                            <td class="sp-column acc-table-td spannerDataType spannerTabCell${tableIndex}${index}"
                                                id="dataType${tableIndex}${index}">
                                                <div class="saveDataType" id="saveDataType${tableIndex}${index}">
                                                    ${spTable .ColDefs[tableColumn].T.Name}</div>
                                                <div class="editDataType template" id="editDataType${tableIndex}${index}">
                                                    <div class="form-group">
                                                        <select class="form-control spanner-input tableSelect"
                                                            id="dataType${tableIndex}${index}${index}">
                                                            <option class="dataTypeOption template"></option>
                                                        </select>
                                                    </div>
                                                </div>
                                            </td>
                                            <td class="acc-table-td">
                                                <select multiple size="1" class="form-control spanner-input tableSelect srcConstraint"
                                                    id="srcConstraint${tableIndex}${index}">
                                                    ${srcTable.ColDefs[currentColumnSrc].NotNull ?
                                                (countSrc[tableIndex][index] = countSrc[tableIndex][index] + 1,
                                                    `<option disabled class="srcNotNullConstraint active">
                                                        Not Null
                                                    </option>`)
                                                :
                                                `<option disabled class="srcNotNullConstraint">
                                                        Not Null
                                                    </option>`}

                                                </select>
                                            </td>
                                            <td class="acc-table-td sp-column acc-table-td spannerTabCell${tableIndex}${index}">
                                                <div class="saveConstraint" id="saveConstraint${tableIndex}${index}">
                                                    <select multiple size="1" class="form-control spanner-input tableSelect spannerConstraint"
                                                        id="spConstraint${tableIndex}${index}">
                                                        ${spTable .ColDefs[tableColumn].NotNull ?
                                                (countSp[tableIndex][index] = countSp[tableIndex][index] + 1,
                                                    notNullConstraint[parseInt(String(tableIndex) + String(index))] = 'Not Null',
                                                    `<option disabled class="active">
                                                            Not Null
                                                        </option>`)
                                                :
                                                (notNullConstraint[parseInt(String(tableIndex) + String(index))] = '',
                                                    `<option disabled>
                                                            Not Null
                                                        </option>`)}
                                                    </select>
                                                </div>
                                            </td>
                                            </tr>`;
                                    }).join("")}
                                    </tbody>
                                </table>
                                ${spTable .Fks?.length > 0 ? this.fkComponent(tableIndex, tableName, spTable .Fks) : `<div></div>`}
                                ${this.secIndexComponent(tableIndex, tableName, spTable .Indexes)}
                                <div class="summaryCard">
                                    <div class="summaryCardHeader" role="tab">
                                        <h5 class="mb-0">
                                            <a data-toggle="collapse" class="summaryFont" href="#viewSummary${tableIndex}">View Summary</a>
                                        </h5>
                                    </div>
                                    <div class="collapse innerSummaryCollapse" id="viewSummary${tableIndex}">
                                        <div class="mdc-card mdc-card-content summaryBorder">
<<<<<<< HEAD
                                            <hb-list-table tabName="summary" dta="${data.summary}" tableName="${tableName}"></hb-list-table>
=======
                                            <hb-list-table tabName="summary" tableName="${tableName}" dta="${data.summary}"></hb-list-table>
>>>>>>> ea16e5459cf3acdcdb24a77c54073039a5fa018c
                                        </div>
                                    </div>
                                </div>
                            </div>`;
        jQuery("#src-sp-table" + tableIndex).DataTable({ "paging": false, "bSort": false });
        tableColumnsArray.map((columnName, index) => {
            new vanillaSelectBox('#srcConstraint' + tableIndex + index, {
                placeHolder: countSrc[tableIndex][index] + " constraints selected",
                maxWidth: 500,
                maxHeight: 300
            });
            new vanillaSelectBox('#spConstraint' + tableIndex + index, {
                placeHolder: countSp[tableIndex][index] + " constraints selected",
                maxWidth: 500,
                maxHeight: 300
            });
        });
        document.getElementById("editSpanner" + tableIndex).addEventListener("click", (event) => {
            Actions.editAndSaveButtonHandler(event, tableIndex, tableName, notNullConstraint);
        });
        if (spTable .Fks !== null && spTable .Fks.length > 0) {
            spTable .Fks.map((fk, index) => {
                document.getElementById(tableName + index + 'foreignKey').addEventListener('click', () => {
                    jQuery('#indexAndKeyDeleteWarning').modal();
                    jQuery('#indexAndKeyDeleteWarning').find('#modal-content').html(`This will permanently delete the foreign key constraint and the corresponding uniqueness constraints on referenced columns. Do you want to continue?`);
                    recreateNode(document.getElementById('fk-drop-confirm'));
                    document.getElementById('fk-drop-confirm').addEventListener('click', () => {
                        Actions.dropForeignKeyHandler(tableName, tableIndex, index);
                    })
                })
            });
        }
        if (spTable .Indexes !== null && spTable .Indexes.length > 0) {
            spTable .Indexes.map((secIndex, index) => {
                document.getElementById(tableName + index + 'secIndex').addEventListener('click', () => {
                    jQuery('#indexAndKeyDeleteWarning').modal();
                    jQuery('#indexAndKeyDeleteWarning').find('#modal-content').html(`This will permanently delete the secondary index and the corresponding uniqueness constraints on indexed columns (if applicable). Do you want to continue?`);
                    recreateNode(document.getElementById('fk-drop-confirm'))
                    document.getElementById('fk-drop-confirm').addEventListener('click', () => {
                        Actions.dropSecondaryIndexHandler(tableName, tableIndex, index);
                    })
                })
            });
        }
    }
    
    constructor() {
        super();
    }
}

window.customElements.define("hb-data-table", DataTable);