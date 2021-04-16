import Actions from "../../services/Action.service.js";
import { tooltipHandler } from "../../helpers/SchemaConversionHelper.js";
import Store from "../../services/Store.service.js";

class EditGlobalDataTypeForm extends HTMLElement {
    connectedCallback() {
        this.render();
        document.getElementById("data-type-button").addEventListener("click", async () => {
                await Actions.setGlobalDataType();
                await Actions.ddlSummaryAndConversionApiCall();
                document.getElementById("app").innerHTML = `<hb-default-layout>
                <hb-schema-conversion-screen></hb-schema-conversion-screen>
                </<hb-default-layout>`;
            });
    }

    render() {
        let globalDataTypeList = Store.getGlobalDataTypeList()
        let dataTypeListKeyArray = Object.keys(globalDataTypeList);
        this.innerHTML = `
        <div class="data-mapping-card" id='globalDataType'>
            <table class='data-type-table' id='globalDataTypeTable'>
                <tbody id='globalDataTypeBody'>
                    <tr>
                        <th>Source</th>
                        <th>Spanner</th>
                    </tr>
                    ${dataTypeListKeyArray.map((dataType, index) => {return `
                    <tr class='globalDataTypeRow' id="dataTypeRow${index + 1}">
                        <td class='src-td' id="dataTypeKey${index + 1}">${dataType}</td>
                        <td id="dataTypeVal${index + 1}">
                            <div class="label-container">
                                <i id="warning${index + 1}" 
                                class="large material-icons warning ${globalDataTypeList[dataType][0].Brief? "":"template"}" 
                                style='cursor: pointer;' data-toggle="tooltip" data-placement="bottom" 
                                title="${globalDataTypeList[dataType][0].Brief}">warning</i>
                        
                                <select class='form-control table-select' 
                                id="dataTypeOption${index + 1}">
                                    ${globalDataTypeList[dataType].map((option, idx) => {return `
                                        <option class='dataTypeOption' value="${option.T}">${option.T}</option>`;
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
            document.getElementById(`dataTypeOption${i + 1}`).addEventListener("change", () => {
                    Actions.dataTypeUpdate(`dataTypeOption${i + 1}`, globalDataTypeList);
                });
        }
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-edit-global-datatype-form",EditGlobalDataTypeForm);
