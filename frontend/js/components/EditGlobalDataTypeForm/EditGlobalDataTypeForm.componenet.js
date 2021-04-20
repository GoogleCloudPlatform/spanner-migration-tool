import Actions from "../../services/Action.service.js";
import { tooltipHandler } from "../../helpers/SchemaConversionHelper.js";

class EditGlobalDataTypeForm extends HTMLElement {
    connectedCallback() {
        this.render();
        document.getElementById("data-type-button").addEventListener("click", async () => {
                await Actions.setGlobalDataType();
                await Actions.ddlSummaryAndConversionApiCall();
            });
    }

    render() {
        let globalDataTypeList = Actions.getGlobalDataTypeList()
        let dataTypeListKeyArray = Object.keys(globalDataTypeList);
        this.innerHTML = `
        <div class="data-mapping-card" id='global-data-type'>
            <table class='data-type-table' id='global-data-type-table'>
                <tbody id='global-data-type-body'>
                    <tr>
                        <th>Source</th>
                        <th>Spanner</th>
                    </tr>
                    ${dataTypeListKeyArray.map((dataType, index) => {return `
                    <tr class='global-data-type-row' id="data-type-row${index + 1}">
                        <td class='src-td' id="data-type-key${index + 1}">${dataType}</td>
                        <td id="data-type-val${index + 1}">
                            <div class="label-container">
                                <i id="warning${index + 1}" 
                                class="large material-icons warning ${globalDataTypeList[dataType][0].Brief? "":"template"}" 
                                style='cursor: pointer;' data-toggle="tooltip" data-placement="bottom" 
                                title="${globalDataTypeList[dataType][0].Brief}">warning</i>
                        
                                <select class='form-control table-select' 
                                id="data-type-option${index + 1}">
                                    ${globalDataTypeList[dataType].map((option, idx) => {return `
                                        <option class='data-type-option' value="${option.T}">${option.T}</option>`;
                                    }).join("")}
                                </select>
                            </div>
                        </td>
                    </tr>`}).join("")}
                </tbody>
            </table>
        </div>`;
        tooltipHandler();
        
        for (let i = 0; i < dataTypeListKeyArray.length; i++) {
            document.getElementById(`data-type-option${i + 1}`).addEventListener("change", () => {
                    Actions.dataTypeUpdate(`data-type-option${i + 1}`, globalDataTypeList);
            });
        }
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-edit-global-datatype-form",EditGlobalDataTypeForm);
