import Forms from "../../services/Forms.service.js";
import Actions from "../../services/Action.service.js";

class ConnectToDbForm extends HTMLElement {

    connectedCallback() {
        this.render();
        let response;
        let modalData = document.getElementById("connectToDbModal");
        modalData.querySelector("i").addEventListener("click", () => {
            Forms.resetConnectToDbModal();
        });
        document.getElementById("db-type").addEventListener("change", () => {
            Forms.toggleDbType();
        });
        document.querySelectorAll("input").forEach(elem => {
            if (elem.type != "submit") {
                elem.addEventListener("focusout", () => {
                    Forms.validateInput(elem, elem.id + "-error");
                });
            }
        });
        Forms.formButtonHandler("connect-form", "connect-button");
        document.getElementById("connect-button").addEventListener("click", async () => {
            Actions.resetStore();
            response = await Actions.onconnect(document.getElementById('db-type').value, document.getElementById('db-host').value,
                document.getElementById('db-port').value, document.getElementById('db-user').value,
                document.getElementById('db-name').value, document.getElementById('db-password').value);
            if (response.ok) {
                document.getElementById("convert-button").addEventListener("click", async () => {
                    await Actions.showSchemaAssessment();
                    await Actions.ddlSummaryAndConversionApiCall();
                    await Actions.setGlobalDataTypeList();
                    window.location.href = '#/schema-report';
                    Actions.sessionRetrieval(Actions.getSourceDbName());
                });
            }
            Forms.resetConnectToDbModal();
        });
    }

    render() {
        this.innerHTML = `
            <div class="form-group">
                <label for="db-type">Database Type</label>
                <select class="form-control db-select-input" id="db-type" name="db-type">
                    <option value="" class="template"></option>
                    <option value="mysql">MySQL</option>
                    <option value="postgres">Postgres</option>
                    <option value='dynamodb'>dynamoDB</option>
                </select>
            </div>
            <div id="sql-fields" class="template">
                <form id="connect-form">
                    <div>
                        <label for="db-host">Database Host</label>
                        <input type="text" class="form-control db-input" name="db-host" id="db-host" autocomplete="off" />
                        <span class='form-error' id='db-host-error'></span><br>
                    </div>
                    <div>
                        <label for="db-port">Database Port</label>
                        <input class="form-control db-input" type="text" name="db-port" id="db-port" autocomplete="off" />
                        <span class='form-error' id='db-port-error'></span><br>
                    </div>
                    <div>
                        <label for="db-user">Database User</label>
                        <input class="form-control db-input" type="text" name="db-user" id="db-user" autocomplete="off" />
                        <span class='form-error' id='db-user-error'></span><br>
                    </div>
                    <div>
                        <label for="db-name">Database Name</label>
                        <input class="form-control db-input" type="text" name="db-name" id="db-name" autocomplete="off" />
                        <span class='form-error' id='db-name-error'></span><br>
                    </div>
                    <div>
                        <label for="db-password">Database Password</label>
                        <input class="form-control db-input" type="password" name="db-password" id="db-password"
                            autocomplete="off" />
                        <span class='form-error' id='db-password-error'></span><br>
                    </div>
                </form>
            </div>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-connect-to-db-form', ConnectToDbForm);