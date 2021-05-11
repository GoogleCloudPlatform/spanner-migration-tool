import "./../Modal/Modal.component.js";
import { recreateNode, checkBoxStateHandler, editButtonHandler } from './../../helpers/SchemaConversionHelper.js';
import "./../SiteButton/SiteButton.component.js"
import Actions from './../../services/Action.service.js';
import { vanillaSelectBox } from './../../../third_party/dummy.js'
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

    set data(value) {
        this._dta = value;
        if (this._dta.SpSchema.Fks) {
            this.checkInterLeave = Actions.getInterleaveConversionForATable(this.tableName);
            if (this.checkInterLeave === undefined) {
                Actions.checkInterleaveConversion(this.tableName);
                this.checkInterLeave = Actions.getInterleaveConversionForATable(this.tableName);
            }
        }
        this.render();
    }

    connectedCallback() {}

    fkComponent(tableIndex, tableName, fkArray, tableMode) {
        return `
            <div class="fk-card">
                <div class="foreign-key-header" role="tab">
                    <h5 class="mb-0">
                        <a class="fk-font" data-toggle="collapse" href="#foreign-key-${tableIndex}"> Foreign Keys </a>
                    </h5>
                </div>
                <div class="collapse fk-collapse show" id="foreign-key-${tableIndex}">
                    <div class="mdc-card mdc-card-content summary-border">
                        <div class="mdc-card fk-content">
                            ${this.checkInterLeave ? `<fieldset id="radio-btn-area${tableIndex}">
                                <div class="radio-class">
                                    <input type="radio" class="radio" value="add" checked="checked" ${tableMode ? "" : "disabled"}
                                        id="add${tableIndex}" name="fks${tableIndex}" />
                                    <label for="add">Use as Foreign Key</label>
                                    <input type="radio" class="radio" value="interleave" ${tableMode ? "" : "disabled"}
                                        id="interleave${tableIndex}" name="fks${tableIndex}" />
                                    <label for="interleave">Convert to Interleave</label>
                                </div>
                            </fieldset>`: `<div></div>`}
                            <br/>
                            <table class="fk-acc-table fk-table">
                                <thead>
                                    <tr>
                                        <th>Name</th>
                                        <th>Columns</th>
                                        <th>Refer Table</th>
                                        <th>Refer Columns</th>
                                        <th>Action</th>
                                    </tr>
                                </thead>
                                <tbody id="fk-table-body-${tableIndex}">
                                    ${fkArray.map((fk, index) => {
            return `
                                    <tr>
                                        <td class="acc-table-td">
                                            ${tableMode ? `<div id="rename-fk-${tableIndex}${index}">
                                                <input type="text" class="form-control spanner-input" autocomplete="off"
                                                    id="new-fk-val-${tableIndex}${index}" value=${fk.Name} />
                                            </div>`
                    : `<div id="save-fk-${tableIndex}${index}" >${fk.Name}</div>`
                }
                                        </td>
                                        <td class="acc-table-td">${fk.Columns[0]}</td>
                                        <td class="acc-table-td">${fk.ReferTable}</td>
                                        <td class="acc-table-td">${fk.ReferColumns[0]}</td>
                                        <td class="acc-table-td">
                                            <button class="drop-button" id="${tableName}${index}foreignKey" data-toggle="tooltip"
                                                data-placement="bottom" title="this will delete foreign key permanently" ${tableMode ? "" : "disabled"}>
                                                <span><i class="large material-icons remove-icon vertical-alignment-middle">delete</i></span>
                                                <span class="vertical-alignment-middle">Drop</span>
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

    secIndexComponent(tableIndex, tableName, secIndexArray, tableMode) {
        return `
            <div>
                <div class="foreign-key-header" role="tab">
                    <h5 class="mb-0">
                        <a class="index-font" data-toggle="collapse" href="#index-key-${tableIndex}">
                            Secondary Indexes
                        </a>
                    </h5>
                </div>
                <div class="collapse index-collapse show" id="index-key-${tableIndex}">
                    <div class="mdc-card mdc-card-content summary-border">
                        <div class="mdc-card fk-content">
                            <hb-site-button buttonid="hb-${tableIndex}indexButton-${this.tableName}" classname="new-index-button"
                                buttonaction="createNewSecIndex" text="Add Index"></hb-site-button>
                            <table class="index-acc-table fk-table">
                                ${ secIndexArray && secIndexArray.length > 0 ? `<thead>
                                    <tr>
                                        <th>Name</th>
                                        <th>Table</th>
                                        <th>Unique</th>
                                        <th>Keys</th>
                                        <th>Action</th>
                                    </tr>
                                </thead>`: `<div></div>`}
                                <tbody>
                                    ${secIndexArray ? secIndexArray?.map((secIndex, index) => {
            return `
                                    <tr>
                                        <td class="acc-table-td">
                                            ${tableMode ? `<div  id="rename-sec-index-${tableIndex}${index}">
                                                <input type="text" id="new-sec-index-val-${tableIndex}${index}" value=${secIndex.Name}
                                                    class="form-control spanner-input" autocomplete="off" />
                                            </div>`
                    : `<div id="save-sec-index-${tableIndex}${index}">${secIndex.Name}</div>`
                }
                                        </td>
                                        <td class="acc-table-td">${secIndex.Table}</td>
                                        <td class="acc-table-td">${secIndex.Unique}</td>
                                        <td class="acc-table-td">${secIndex?.Keys.map((key) => key.Col).join(',')}</td>
                                        <td class="acc-table-td">
                                            <button class="drop-button" id="${tableName}${index}secIndex" data-toggle="tooltip"
                                                data-placement="bottom" title="this will delete secondary index permanently"
                                                ${tableMode ? "" : "disabled"}>
                                                <span><i
                                                        class="large material-icons remove-icon vertical-alignment-middle">delete</i></span>
                                                <span class="vertical-alignment-middle">Drop</span>
                                            </button>
                                        </td>
                                    </tr>
                                    `;
        }).join("") : ``}
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
        let tableColumnsArrayLength = tableColumnsArray.length;
        let pksSp = [...spTable.Pks];
        let pksSpLength = pksSp.length;
        let pkSeqId = 1;
        countSrc[tableIndex] = [];
        countSp[tableIndex] = [];
        for (var x = 0; x < pksSpLength; x++) {
            if (pksSp[x].seqId == undefined) {
                pksSp[x].seqId = pkSeqId;
                pkSeqId++;
            }
        }
        let sourceDbName = Actions.getSourceDbName();
        let tableMode = Actions.getTableMode(tableIndex);
        let dataTypesarray = Actions.getGlobalDataTypeList();
        let pageNumber = data.currentPageNumber;
        let columnPerPage = 15;
        let maxPossiblePageNumber =  Math.ceil(tableColumnsArrayLength / columnPerPage);
        let tableColumnsArrayCurrent = [];
        let z = 0;
        for(let i= pageNumber*columnPerPage ;i < Math.min(tableColumnsArrayLength, pageNumber*columnPerPage+15) ; i++ )
        {
            tableColumnsArrayCurrent[z]=tableColumnsArray[i];
            z++;
        }

        this.innerHTML =
            ` <div class="acc-card-content" id="acc-card-content">
                <table class="acc-table" id="src-sp-table${tableIndex}">
                    <thead>
                        <tr>
                            <th class="acc-column" colspan="2">Column Name</th>
                            <th class="acc-column" colspan="2">Data Type</th>
                            <th class="acc-column" colspan="2">Constraints</th>
                        </tr>
                        <tr>
                            <th class="acc-table-th-src src-tab-cell">
                                ${tableMode ? `<span class="bmd-form-group is-filled">
                                    <div class="checkbox">
                                        <label>
                                            <input type="checkbox" value="" id="chck-all-${tableIndex}" checked=true/>
                                            <span class="checkbox-decorator">
                                                <span class="check ml7"></span>
                                                <div class="ripple-container"></div>
                                            </span>
                                        </label>
                                    </div>
                                </span>`: `<div></div>`
            }
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
                        
                        ${
                            // filter((_, idx) => idx >= pageNumber * columnPerPage && idx < pageNumber * columnPerPage + columnPerPage)          
                tableColumnsArrayCurrent.map((tableColumn, index) => {
                let pkFlag = false, seqId;
                countSrc[tableIndex][index] = 0;
                countSp[tableIndex][index] = 0;
                for (var x = 0; x < pksSpLength; x++) {
                    if (pksSp[x].Col === tableColumn) {
                        pkFlag = true; seqId = pksSp[x].seqId;
                        break
                    }
                } let currentColumnSrc = data.ToSource.Cols[tableColumn];
                let defaultdatatype = spTable.ColDefs[tableColumn].T.Name;
                return `
                            <tr class="report-table-content">
                            <td class="acc-table-td src-tab-cell">
                                ${tableMode ? `<span class="bmd-form-group is-filled each-row-chck-box">
                                    <div class="checkbox">
                                        <label>
                                            <input type="checkbox" value="" id="chck-box-${tableIndex}"
                                                class="chck-class-${tableIndex}" checked=true/>
                                            <span class="checkbox-decorator"><span class="check"></span>
                                                <div class="ripple-container"></div>
                                            </span>
                                        </label>
                                    </div>
                                </span>`: `<div></div>`}
                                <span class="column left">
                                    ${(currentColumnSrc != srcTable.PrimaryKeys[0].Column || srcTable.PrimaryKeys === null) ?
                        `<img class="hidden ml-3" src="./Icons/Icons/ic_vpn_key_24px.svg" />` :
                        `<img class="ml-3" src="./Icons/Icons/ic_vpn_key_24px.svg" />`}
                                </span>
                                <span class="column right src-column"
                                    id="src-column-name-${tableIndex}${index}${index}">${currentColumnSrc}</span>
                            </td>
                            <td class="sp-column acc-table-td spanner-tab-cell-${tableIndex}${index}">
                                ${tableMode ? `<div class="edit-column-name " id="edit-column-name-${tableIndex}${index}">
                                    <span class="column left key-margin">
                                        ${pkFlag ?
                            `<sub>${seqId}</sub>
                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primary-key" />` :
                            `<sub></sub>
                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primary-key hidden" />`}
                                    </span>
                                    <span class="column right form-group">
                                        <input type="text" class="spanner-input form-control"
                                            id="column-name-text-${tableIndex}${index}${index}" autocomplete="off"
                                            value=${tableColumn} />
                                    </span>
                                </div>`
                        : `
                                <div id="save-column-name-${tableIndex}${index}">
                                    <span class="column left pointer">
                                        ${pkFlag ?
                            `<sub>${seqId}</sub>
                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primary-key" />` :
                            `<sub></sub>
                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primary-key hidden" />`}
                                    </span>
                                    <span class="column right spanner-col-name-span pointer">${tableColumn}</span>
                                </div>`}
                            </td>
                            <td class="acc-table-td" id="src-data-type-${tableIndex}${index}">
                                ${srcTable.ColDefs[currentColumnSrc]?.Type.Name}</td>
                            <td class="sp-column acc-table-td spanner-tab-cell-${tableIndex}${index}"
                                id="data-type-${tableIndex}${index}">
                                
                                ${tableMode ? `<div  id="edit-data-type-${tableIndex}${index}">
                                    <div class="form-group">
                                        <select class="form-control spanner-input report-table-select"
                                            id="data-type-${tableIndex}${index}${index}">
                                            ${
                        dataTypesarray[srcTable.ColDefs[currentColumnSrc].Type.Name]?.map((type) => {
                            return `<option class="data-type-option" value="${type.T}" ${defaultdatatype == type.T ? "selected" : ""}>${type.T}</option>`;
                        }).join('')
                        }
                                            
                                        </select>
                                    </div>
                                </div>
                                `: `
                                <div id="save-data-type-${tableIndex}${index}">
                                ${defaultdatatype}</div>`
                    }
                            </td>
                            <td class="acc-table-td">
                                <select multiple size="1" class="form-control spanner-input report-table-select srcConstraint"
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
                            <td class="acc-table-td sp-column acc-table-td spanner-tab-cell-${tableIndex}${index}">
                                <div>
                                    <select multiple size="1" class="form-control spanner-input report-table-select"
                                        id="sp-constraint-${tableIndex}${index}">
                                        ${spTable.ColDefs[tableColumn].NotNull ?
                        (countSp[tableIndex][index] = countSp[tableIndex][index] + 1,
                            notNullConstraint[parseInt(String(tableIndex) + String(index))] = 'Not Null',
                            `<option ${tableMode ? "" : "disabled"} class="active">
                                            Not Null
                                        </option>`)
                        :
                        (notNullConstraint[parseInt(String(tableIndex) + String(index))] = '',
                            `<option ${tableMode ? "" : "disabled"}>
                                            Not Null
                                        </option>`)}
                                    </select>
                                </div>
                            </td>
                            </tr>`;
            }).join("")}
                    </tbody>
                </table>
                <div class="pagination-container">
                    <span class="pagination-text">Showing ${pageNumber * columnPerPage + 1} to ${Math.min(pageNumber * columnPerPage + columnPerPage, tableColumnsArrayLength)} of ${tableColumnsArrayLength} entries </span>
                    <div>
                        <button  class="pagination-button" id="pre-btn${tableIndex}" ${pageNumber <= 0 ? `disabled` : ``}>&#8592; Pre </button>
                        <input class="pagination-input" type="number" min="1" max="${ maxPossiblePageNumber}" value="${parseInt(pageNumber)+1}" id="pagination-input-id-${tableIndex}" />
                        <span  class="pagination-number">/${ maxPossiblePageNumber}</span>
                        <button  class="pagination-button" id="next-btn${tableIndex}" ${(pageNumber + 1) * columnPerPage >= tableColumnsArrayLength ? `disabled` : ``}> Next &#8594; </button>
                    </div>
                </div>
            ${spTable.Fks?.length > 0 ? this.fkComponent(tableIndex, tableName, spTable.Fks, tableMode) : `<div></div>`}
            ${this.secIndexComponent(tableIndex, tableName, spTable.Indexes, tableMode)}
            <div class="summary-card">
                <div class="summary-card-header" role="tab">
                    <h5 class="mb-0">
                        <a data-toggle="collapse" class="summary-font" href="#view-summary-${tableIndex}">View Summary</a>
                    </h5>
                </div>
                <div class="collapse inner-summary-collapse" id="view-summary-${tableIndex}">
                    <div class="mdc-card mdc-card-content summary-border">
                        <hb-list-table tabName="summary" tableName="${tableName}" dta="${data.summary}"></hb-list-table>
                    </div>
                </div>
            </div>
        </div>`;

        jQuery("#src-sp-table" + tableIndex).DataTable({ "paging": false, "bSort": false });
        tableColumnsArrayCurrent.map((columnName, index) => {
            new vanillaSelectBox('#srcConstraint' + tableIndex + index, {
                placeHolder: countSrc[tableIndex][index] + " constraints selected",
                maxWidth: 500,
                maxHeight: 300
            });
            new vanillaSelectBox('#sp-constraint-' + tableIndex + index, {
                placeHolder: countSp[tableIndex][index] + " constraints selected",
                maxWidth: 500,
                maxHeight: 300
            });
        });
        document.getElementById("editSpanner" + tableIndex)?.addEventListener("click", async (event) => {
                if(event.target.innerHTML.trim()=="Edit Spanner Schema") {
                    Actions.showSpinner();
                    Actions.setTableMode(tableIndex,true);
                }
                else {
                    Actions.showSpinner();
                   await Actions.SaveButtonHandler(tableIndex, tableName, notNullConstraint);
                }
        });
        document.getElementById("pagination-input-id-"+tableIndex).addEventListener("keypress",(e)=>{
            if(e.key === "Enter" && e.target.value != pageNumber+1){
                if(e.target.value>0 && e.target.value<=  maxPossiblePageNumber ) {
                    Actions.showSpinner();
                    Actions.changePage(tableIndex,parseInt(e.target.value)-1);
                }
                else if(e.target.value > maxPossiblePageNumber ){
                    document.getElementById("pagination-input-id-"+tableIndex).value = maxPossiblePageNumber;
                    Actions.showSpinner();
                    Actions.changePage(tableIndex,parseInt(maxPossiblePageNumber)-1);
                }
                else if(e.target.value < 1){
                    document.getElementById("pagination-input-id-"+tableIndex).value = 1;
                    Actions.showSpinner();
                    Actions.changePage(tableIndex,0);
                }
            }
        })

        if (spTable.Fks !== null && spTable.Fks.length > 0) {
            spTable.Fks.map((fk, index) => {
                document.getElementById(tableName + index + 'foreignKey').addEventListener('click', () => {
                    jQuery('#index-and-key-delete-warning').modal();
                    jQuery('#index-and-key-delete-warning').find('#modal-content').html(`This will permanently delete the foreign key
                    constraint and the corresponding uniqueness constraints on referenced columns. Do you want to continue?`);
                    recreateNode(document.getElementById('fk-drop-confirm'));
                    document.getElementById('fk-drop-confirm').addEventListener('click', () => {
                        Actions.dropForeignKeyHandler(tableName, tableIndex, index);
                    })
                })
            });
        }
        if (spTable.Indexes !== null && spTable.Indexes.length > 0) {
            spTable.Indexes.map((secIndex, index) => {
                document.getElementById(tableName + index + 'secIndex').addEventListener('click', () => {
                    jQuery('#index-and-key-delete-warning').modal();
                    jQuery('#index-and-key-delete-warning').find('#modal-content').html(`This will permanently delete the secondary index
                    and the corresponding uniqueness constraints on indexed columns (if applicable). Do you want to continue?`);
                    recreateNode(document.getElementById('fk-drop-confirm'))
                    document.getElementById('fk-drop-confirm').addEventListener('click', () => {
                        Actions.dropSecondaryIndexHandler(tableName, tableIndex, index);
                    })
                })
            });
        }

        document.getElementById('pre-btn' + tableIndex).addEventListener('click', async () => {
            Actions.showSpinner()
            let columnStatus = true;
            if (tableMode) {
                let errorMessage = [], updateInStore = true;
                columnStatus = await Actions.saveColumn({}, tableIndex, tableName, notNullConstraint, { data: {} }, errorMessage, updateInStore);
            }
            if (columnStatus) Actions.decrementPageNumber(tableIndex)
        })

        document.getElementById('next-btn' + tableIndex).addEventListener('click', async () => {
            Actions.showSpinner()
            let columnStatus = true;
            if (tableMode) {
                let errorMessage = [], updateInStore = true;
                columnStatus = await Actions.saveColumn({}, tableIndex, tableName, notNullConstraint, { data: {} }, errorMessage, updateInStore);
            }
            if (columnStatus) Actions.incrementPageNumber(tableIndex)
        })

        if (tableMode) {
            checkBoxStateHandler(tableIndex, Object.keys(data.ToSpanner.Cols).length);
            editButtonHandler(tableIndex, notNullConstraint);
        }
        document.getElementById(`src-sp-table${tableIndex}`)?.style.removeProperty('width');
        document.getElementById(`src-sp-table${tableIndex}_info`).style.display = "none"
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-data-table", DataTable);