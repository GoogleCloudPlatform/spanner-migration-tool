import Forms from "../../services/Forms.service.js";
import Actions from "../../services/Action.service.js";

class EditGlobalDataTypeForm extends HTMLElement {

    connectedCallback() {
        this.render();
        document.getElementById('data-type-button').addEventListener('click' ,() => {
            await Actions.setGlobalDataType()
            await this.ddlSummaryAndConversionApiCall()
            location.reload()
        } )
    }

    render() {
        this.innerHTML = `
                <div class="data-mapping-card" id='globalDataType'>
                <table class='data-type-table' id='globalDataTypeTable'>
                    <tbody id='globalDataTypeBody'>
                        <tr>
                            <th>Source</th>
                            <th>Spanner</th>
                        </tr>
                        <tr class='globalDataTypeRow template'>
                            <td class='src-td'></td>
                            <td id='globalDataTypeCell'>
                                <div style='display: flex;'>
                                    <i class="large material-icons warning" style='cursor: pointer;'>warning</i>
                                    <select class='form-control table-select' style='border: 0px !important;'>
                                        <option class='dataTypeOption template'></option>
                                    </select>
                                </div>
                            </td>
                        </tr>
                    </tbody>
                </table>

            </div>
        `;}

    constructor() {
        super();
    }
}

window.customElements.define('hb-edit-global-datatype-form', EditGlobalDataTypeForm);