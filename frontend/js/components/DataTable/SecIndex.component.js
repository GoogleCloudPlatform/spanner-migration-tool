class SecIndex extends HTMLElement {
    get secIndexId() {
      return this.getAttribute("secIndexId");
    }
  
    get secIndexArray() {
      return JSON.parse(this.getAttribute("secIndexArray"));
    }
  
    attributeChangedCallback(name, oldValue, newValue) {
      this.render();
    }
  
    connectedCallback() {
      this.render();
    }
  
    render() {
      const { secIndexId, secIndexArray } = this;
      this.innerHTML = `
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
                                    <button class="newIndexButton" onclick="createNewSecIndex(this.id)">Add Index</button>
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

}
window.customElements.define('hb-data-table-secindex', SecIndex);