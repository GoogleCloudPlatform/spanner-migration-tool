import Forms from "../../services/Forms.service.js";

class ConnectToDbForm extends HTMLElement {

    connectedCallback() {
        this.render();
        let modalData = document.getElementById("connectToDbModal");
        modalData.querySelector("i").addEventListener("click", () => {
            Forms.resetConnectToDbModal();
        }); 
        document.getElementById("dbType").addEventListener("change", () => {
            Forms.toggleDbType();
        });
        document.querySelectorAll("input").forEach(elem => {
            elem.addEventListener("focusout", () => {
                Forms.validateInput(elem, elem.id + "Error");
            })
        });
        Forms.formButtonHandler("connectForm", "connectButton");
    }

    render() {
        this.innerHTML = `
            <div class="modal-body">
                <div class="form-group">
                    <label for="dbType" class="">Database Type</label>
                    <select class="form-control db-select-input" id="dbType" name="dbType">
                        <option value="" class="template"></option>
                        <option class="db-option" value="mysql">MySQL</option>
                        <option class="db-option" value="postgres">Postgres</option>
                        <option class='db-option' value='dynamodb'>dynamoDB</option>
                    </select>
                </div>
                <div id="sqlFields" class="template">
                    <form id="connectForm">
                        <div class="form-group">
                            <label class="modal-label" for="dbHost">Database Host</label>
                            <input type="text" class="form-control db-input" aria-describedby="" name="dbHost" id="dbHost"
                                autocomplete="off" />
                            <span class='formError' id='dbHostError'></span><br>
                        </div>
                        <div class="form-group">
                            <label class="modal-label" for="dbPort">Database Port</label>
                            <input class="form-control db-input" aria-describedby="" type="text" name="dbPort" id="dbPort"
                                autocomplete="off" />
                            <span class='formError' id='dbPortError'></span><br>
                        </div>
                        <div class="form-group">
                            <label class="modal-label" for="dbUser">Database User</label>
                            <input class="form-control db-input" aria-describedby="" type="text" name="dbUser" id="dbUser"
                                autocomplete="off" />
                            <span class='formError' id='dbUserError'></span><br>
                        </div>
                        <div class="form-group">
                            <label class="modal-label" for="dbName">Database Name</label>
                            <input class="form-control db-input" aria-describedby="" type="text" name="dbName" id="dbName"
                                autocomplete="off" />
                            <span class='formError' id='dbNameError'></span><br>
                        </div>
                        <div class="form-group">
                            <label class="modal-label" for="dbPassword">Database Password</label>
                            <input class="form-control db-input" aria-describedby="" type="password" name="dbPassword" id="dbPassword"
                                autocomplete="off" />
                            <span class='formError' id='dbPasswordError'></span><br>
                        </div>
                    </form>
                </div>
            </div>
            <div id="sqlFieldsButtons" class="template">
                <div class="modal-footer">
                <input type="submit" disabled="disabled" value="Connect" id="connectButton" class="connectButton"
                    onclick="onconnect( document.getElementById('dbType').value, document.getElementById('dbHost').value, document.getElementById('dbPort').value, document.getElementById('dbUser').value, document.getElementById('dbName').value, document.getElementById('dbPassword').value)" />
                </div>
            </div>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-connect-to-db-form', ConnectToDbForm);