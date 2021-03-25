class ConnectToDbForm extends HTMLElement {

    connectedCallback() {
        this.render();
    }

    render() {
        this.innerHTML = `
            <div class="form-group">
                <label for="dbType" class="">Database Type</label>
                <select class="form-control db-select-input" id="dbType" name="dbType" onchange="toggleDbType()">
                    <option value="" style="display: none;"></option>
                    <option class="db-option" value="mysql">MySQL</option>
                    <option class="db-option" value="postgres">Postgres</option>
                    <option class='db-option' value='dynamodb'>dynamoDB</option>
                </select>
            </div>
            <div id="sqlFields" style="display: none;">
                <form id="connectForm">
                    <div class="form-group">
                        <label class="modal-label" for="dbHost">Database Host</label>
                        <input type="text" class="form-control db-input" aria-describedby="" name="dbHost" id="dbHost"
                            autocomplete="off" onfocusout="validateInput(document.getElementById('dbHost'), 'dbHostError')" />
                        <span class='formError' id='dbHostError'></span><br>
                    </div>
                    <div class="form-group">
                        <label class="modal-label" for="dbPort">Database Port</label>
                        <input class="form-control db-input" aria-describedby="" type="text" name="dbPort" id="dbPort"
                            autocomplete="off" onfocusout="validateInput(document.getElementById('dbPort'), 'dbPortError')" />
                        <span class='formError' id='dbPortError'></span><br>
                    </div>
                    <div class="form-group">
                        <label class="modal-label" for="dbUser">Database User</label>
                        <input class="form-control db-input" aria-describedby="" type="text" name="dbUser" id="dbUser"
                            autocomplete="off" onfocusout="validateInput(document.getElementById('dbUser'), 'dbUserError')" />
                        <span class='formError' id='dbUserError'></span><br>
                    </div>
                    <div class="form-group">
                        <label class="modal-label" for="dbName">Database Name</label>
                        <input class="form-control db-input" aria-describedby="" type="text" name="dbName" id="dbName"
                            autocomplete="off" onfocusout="validateInput(document.getElementById('dbName'), 'dbNameError')" />
                        <span class='formError' id='dbNameError'></span><br>
                    </div>
                    <div class="form-group">
                        <label class="modal-label" for="dbPassword">Database Password</label>
                        <input class="form-control db-input" aria-describedby="" type="password" name="dbPassword" id="dbPassword"
                            autocomplete="off" onfocusout="validateInput(document.getElementById('dbPassword'), 'dbPassError')" />
                        <span class='formError' id='dbPassError'></span><br>
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