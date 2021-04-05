import Actions from './../../services/Action.service.js';
import "./../Modal/Modal.component.js"
import "./../AddIndexForm/AddIndexForm.component.js"

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

    async connectedCallback() {
        let spTable = this.schemaConversionObj.SpSchema[this.tableName];
        if(spTable.Fks){
        Actions.checkInterleaveConversion(this.tableName).then((response)=>{
            this.InterleaveApiCallResp=response.tableInterleaveStatus;
            this.render();
        })
        .catch((err)=>{
            console.log(err);
        }); //catch error also
        } else {
        this.render();
        }      
    }

    fkComponent(fkId,fkArray) {
        
        return `
        <div class="fkCard " style="border-radius: 0px !important">
                        <div class="foreignKeyHeader" role="tab">
                            <h5 class="mb-0">
                                <a class="fkFont" data-toggle="collapse" href="#fk-${fkId}"> Foreign Keys </a>
                            </h5>
                        </div>
                        <div class="collapse fkCollapse" id="fk-${fkId}">
                            <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                <div class="mdc-card fk-content">
                                    <fieldset class=${this.InterleaveApiCallResp.Possible?"":"template"}>
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
    
    secIndexComponent(secIndexId,secIndexArray) {

        return `
        <hb-modal modalId="createIndexModal${secIndexId}" 
    content="<hb-add-index-form tableName='${this.tableName}'></hb-add-index-form>" 
    contentIcon="" connectIconClass="" modalBodyClass="" 
    title="Select keys for new index"></hb-modal>

        <div class="indexesCard " style="border-radius: 0px !important">
                              <div class="foreignKeyHeader" role="tab">
                                  <h5 class="mb-0">
                                      <a class="indexFont" data-toggle="collapse" href="#secindex-${secIndexId}">
                                          Secondary Indexes
                                      </a>
                                  </h5>
                              </div>
                              <div class="collapse indexCollapse" id="secindex-${secIndexId}">
                                  <div class="mdc-card mdc-card-content summaryBorder" style="border: 0px">
                                      <div class="mdc-card fk-content">
                                          <hb-site-button buttonid="createIndexModal${secIndexId}" classname="newIndexButton" buttonaction="createNewSecIndex" text="Add Index"></hb-site-button>

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
      
                                                  ${ secIndexArray.map((eachsecIndex)=>{
                                                      return `
                                                      <tr class="indexTableTr ">
                                                      <td class="acc-table-td indexesName">
                                                          <div class="renameSecIndex template">
                                                              <input type="text" class="form-control spanner-input"
                                                                  autocomplete="off" />
                                                          </div>
                                                          <div class="saveSecIndex">${eachsecIndex.Name}</div>
                                                      </td>
                                                      <td class="acc-table-td indexesTable">${eachsecIndex.Table}</td>
                                                      <td class="acc-table-td indexesUnique">${eachsecIndex.Unique}</td>
                                                      <td class="acc-table-td indexesKeys">${eachsecIndex.Keys.map((key)=>key.Col).join(',')}</td>
                                                      <td class="acc-table-td indexesAction">
                                                          <button class="dropButton" disabled>
                                                              <span><i class="large material-icons removeIcon"
                                                                      style="vertical-align: middle">delete</i></span>
                                                              <span style="vertical-align: middle">Drop</span>
                                                          </button>
                                                      </td>
                                                  </tr>
                                                           
                                                      `;
                                                  }).join("")
      
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
        let { tableName, tableIndex } = this;
        let countSrc = [], countSp = [];
        countSrc[tableIndex] = [];
        countSp[tableIndex] = [];
        let spTable = this.schemaConversionObj.SpSchema[tableName];
        let srcTable = this.schemaConversionObj.SrcSchema[tableName];
        let tableColumnsArray = Object.keys(this.schemaConversionObj.ToSpanner[spTable.Name].Cols);
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
            let currentColumnSrc = Object.keys(this.schemaConversionObj.ToSpanner[spTable.Name].Cols)[index];
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
                      spTable.Fks? this.fkComponent(tableIndex,spTable.Fks):`<div></div>`
                    }

                    ${
                      spTable.Indexes?this.secIndexComponent(tableIndex ,spTable.Indexes):`<div></div>`
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

    constructor () {
        super();
        this.schemaConversionObj = JSON.parse(localStorage.getItem("conversionReportContent"));

    }
}

window.customElements.define("hb-data-table", DataTable);
