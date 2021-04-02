import "./Fk.component.js";
import "./SecIndex.component.js";

class DataTable extends HTMLElement {
    static get observedAttributes() {
        return ["open"];
    }

    get tableName() {
        return this.getAttribute("tableName");
    }

    get tableIndex() {
        return this.getAttribute("tableIndex");
    }

    attributeChangedCallback(name, oldValue, newValue) {
        this.render();
    }

    connectedCallback() {
        this.render();
    }

    render() {
        let { tableName, tableIndex } = this;
        let countSrc = [], countSp = [];
        countSrc[tableIndex] = [];
        countSp[tableIndex] = [];
        let schemaConversionObj = JSON.parse(localStorage.getItem("conversionReportContent"));
        let spTable = schemaConversionObj.SpSchema[tableName];
        let srcTable = schemaConversionObj.SrcSchema[tableName];
        let tableColumnsArray = Object.keys(schemaConversionObj.ToSpanner[spTable.Name].Cols);
        let pksSp = [...spTable.Pks];
        let pksSpLength = pksSp.length;
        let pkSeqId = 1;
        for (var x = 0; x < pksSpLength; x++) {
            if (pksSp[x].seqId == undefined) {
                pksSp[x].seqId = pkSeqId;
                pkSeqId++;
            }
        }
        this.innerHTML = `
        <div class="acc-card-content" id="acc_card_content">
                    <table class="acc-table" id="src-sp-table${tableIndex}">
                        <thead>
                            <tr>
                                <th class="acc-column" colspan="2">Column Name</th>
                                <th class="acc-column" colspan="2">Data Type</th>
                                <th class="acc-column" colspan="2">Constraints</th>
                            </tr>
                            <tr>
                                <th class="acc-table-th-src src-tab-cell">
                                    <span class="bmd-form-group is-filled">
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
                                    ${localStorage.getItem('sourceDbName')}
                                </th>
                                <th class="acc-table-th-spn">Spanner</th>
                                <th class="acc-table-th-src">${localStorage.getItem('sourceDbName')}</th>
                                <th class="acc-table-th-spn">Spanner</th>
                                <th class="acc-table-th-src">${localStorage.getItem('sourceDbName')}</th>
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
                    pkFlag = true;
                    seqId = pksSp[x].seqId;
                    break
                }
            }
            let currentColumnSrc = Object.keys(schemaConversionObj.ToSpanner[spTable.Name].Cols)[index];
            return `
                            <tr class="reportTableContent">
                            <td class="acc-table-td src-tab-cell">
                                <span class="bmd-form-group is-filled eachRowChckBox">
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
                                
                                    ${(currentColumnSrc != srcTable.PrimaryKeys[0].Column || srcTable.PrimaryKeys === null)
                    ? `<img class="srcPk hidden ml-3" src="./Icons/Icons/ic_vpn_key_24px.svg" />` :
                    `<img class="srcPk ml-3" src="./Icons/Icons/ic_vpn_key_24px.svg" />`}
                                    
                                </span>
                                <span class="column right srcColumn">${currentColumnSrc}</span>
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
                                <div class="saveColumnName">
                                    <span class="column left spannerPkSpan">
                                        ${pkFlag ?
                    `<sub>${seqId}</sub>
                                            <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey" />` :
                    `<sub></sub>
                                        <img src="./Icons/Icons/ic_vpn_key_24px.svg" class="primaryKey hidden" />`}
                                        
                                    </span>
                                    <span class="column right spannerColNameSpan">${tableColumn}</span>
                                </div>
                            </td>
                            <td class="acc-table-td srcDataType">${srcTable.ColDefs[currentColumnSrc].Type.Name}</td>
                            <td class="sp-column acc-table-td spannerDataType">
                                <div class="saveDataType">${spTable.ColDefs[tableColumn].T.Name}</div>
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
                                    class="form-control spanner-input tableSelect srcConstraint" id="srcConstraint${tableIndex}${index}">
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
                            <td class="acc-table-td sp-column acc-table-td">
                                <div class="saveConstraint">
                                    <select multiple size="1"
                                        class="form-control spanner-input tableSelect spannerConstraint" id="spConstraint${tableIndex}${index}">
                                        ${spTable.ColDefs[tableColumn].NotNull ?
                                            (countSp[tableIndex][index] = countSp[tableIndex][index] + 1,
                                                `<option disabled class="spannerNotNullConstraint active">
                                                Not Null
                                            </option>`)
                                        :
                                        `<option disabled class="spannerNotNullConstraint">
                                        Not Null
                                    </option>`}
                                    </select>
                                </div>
                            </td>
                        </tr>`;
        }).join("")}
        
                        </tbody>
                    </table>
                    ${
                      spTable.Fks?`<hb-data-table-fk fkId=${tableIndex} fkArray=${JSON.stringify(spTable.Fks)} ></hb-data-table-fk>`:`<div></div>`
                    }

                    ${
                      spTable.Indexes?`<hb-data-table-secindex secIndexId=${tableIndex} secIndexArray=${JSON.stringify(spTable.Indexes)} ></hb-data-table-secindex>`:`<div></div>`
                    }
                    
                    <div class="summaryCard">
                        <div class="summaryCardHeader" role="tab">
                            <h5 class="mb-0">
                                <a data-toggle="collapse" class="summaryFont" href="#summary-${tableIndex}">View Summary</a>
                            </h5>
                        </div>
                        <div class="collapse innerSummaryCollapse" id="summary-${tableIndex}">
                            <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                <hb-list-table tabName="summary" tableName="${tableName}"></hb-list-table>
                            </div>
                        </div>
                    </div>
                </div>`;
        jQuery("#src-sp-table" + tableIndex).DataTable({ "paging": false });
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
        })
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-data-table", DataTable);
