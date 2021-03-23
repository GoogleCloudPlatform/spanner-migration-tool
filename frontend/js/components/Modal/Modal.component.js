const DB_TYPE_LABEL = "Database Type";
const SESSION_FILE_PATH_LABEL = "Path of the session File";
const DUMP_FILE_PATH_LABEL = "Path of the Dump File";
const DB_HOST_LABEL = "Database Host";
const DB_PORT_LABEL = "Database Port";
const DB_USER_LABEL = "Database User";
const DB_NAME_LABEL = "Database Name";
const DB_PASS_LABEL = "Database Password";
const LOAD_SESSION_FILE_TITLE = "Load Session File";
const LOAD_DUMP_FILE_TITLE = "Load Database Dump";
const MYSQL_DB = "MySQL";
const POSTGRES_DB = "Postgres";

class Modal extends HTMLElement {

  connectedCallback() {
    this.render();
  }

  render() {
    this.innerHTML = `
      <div class="modal" id="connectToDbModal" tabindex="-1" role="dialog" aria-labelledby="exampleModalCenterTitle"
        aria-hidden="true">
        <div class="modal-dialog modal-dialog-centered" role="document">
          <div class="modal-content">
            <div class="modal-header content-center">
              <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Connect to Database</h5>
              <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
            </div>
            <div class="modal-body">
              <div class="form-group">
                <label for="dbType" class="">${DB_TYPE_LABEL}</label>
                <select class="form-control db-select-input" id="dbType" name="dbType" onchange="toggleDbType()">
                  <option value="" class="template"></option>
                  <option class="db-option" value="mysql">${MYSQL_DB}</option>
                  <option class="db-option" value="postgres">${POSTGRES_DB}</option>
                </select>
              </div>
              <div id="sqlFields" class="template">
                <form id="connectForm">
                  <div class="form-group">
                    <label class="modal-label" for="dbHost">${DB_HOST_LABEL}</label>
                    <input type="text" class="form-control db-input" aria-describedby="" name="dbHost" id="dbHost"
                      autocomplete="off" onfocusout="validateInput(document.getElementById('dbHost'), 'dbHostError')" />
                    <span class='formError' id='dbHostError'></span><br>
                  </div>
                  <div class="form-group">
                    <label class="modal-label" for="dbPort">${DB_PORT_LABEL}</label>
                    <input class="form-control db-input" aria-describedby="" type="text" name="dbPort" id="dbPort"
                      autocomplete="off" onfocusout="validateInput(document.getElementById('dbPort'), 'dbPortError')" />
                    <span class='formError' id='dbPortError'></span><br>
                  </div>
                  <div class="form-group">
                    <label class="modal-label" for="dbUser">${DB_USER_LABEL}</label>
                    <input class="form-control db-input" aria-describedby="" type="text" name="dbUser" id="dbUser"
                      autocomplete="off" onfocusout="validateInput(document.getElementById('dbUser'), 'dbUserError')" />
                    <span class='formError' id='dbUserError'></span><br>
                  </div>
                  <div class="form-group">
                    <label class="modal-label" for="dbName">${DB_NAME_LABEL}</label>
                    <input class="form-control db-input" aria-describedby="" type="text" name="dbName" id="dbName"
                      autocomplete="off" onfocusout="validateInput(document.getElementById('dbName'), 'dbNameError')" />
                    <span class='formError' id='dbNameError'></span><br>
                  </div>
                  <div class="form-group">
                    <label class="modal-label" for="dbPassword">${DB_PASS_LABEL}</label>
                    <input class="form-control db-input" aria-describedby="" type="password" name="dbPassword" id="dbPassword"
                      autocomplete="off" onfocusout="validateInput(document.getElementById('dbPassword'), 'dbPassError')" />
                    <span class='formError' id='dbPassError'></span><br>
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
          </div>
        </div>
      </div>

      <div id="loadDatabaseDumpModal" class="modal loadDatabaseDumpModal" tabindex="-1" role="dialog"
        aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
        <div class="modal-dialog modal-dialog-centered" role="document">
          <!-- Modal content-->
          <div class="modal-content">
            <div class="modal-header content-center">
              <h5 class="modal-title modal-bg" id="exampleModalLongTitle">${LOAD_DUMP_FILE_TITLE}</h5>
              <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
            </div>
            <div class="modal-body">
              <!-- <form id="loadDbForm"> -->
              <div class="form-group">
                <label class="" for="loadDbType">${DB_TYPE_LABEL}</label>
                <select class="form-control load-db-input" id="loadDbType" name="loadDbType">
                  <option value="" class="template"></option>
                  <option class="db-option" value="mysql">${MYSQL_DB}</option>
                  <option class="db-option" value="postgres">${POSTGRES_DB}</option>
                </select>
              </div>
              <form id="loadDbForm">
                <div class="form-group">
                  <label class="modal-label" for="dumpFilePath">${DUMP_FILE_PATH_LABEL}</label>
                  <input class="form-control load-db-input" aria-describedby="" type="text" name="dumpFilePath"
                    id="dumpFilePath" autocomplete="off"
                    onfocusout="validateInput(document.getElementById('dumpFilePath'), 'filePathError')" />
                  <span class='formError' id='filePathError'></span>
                </div>
                <input type="text" class="template">
              </form>
            </div>
            <div class="modal-footer">
              <input type="submit" disabled='disabled' value='Confirm' id='loadConnectButton' class='connectButton'
                onclick='storeDumpFileValues(document.getElementById("loadDbType").value, document.getElementById("dumpFilePath").value)' />
            </div>
          </div>
        </div>
      </div>

      <div id="importSchemaModal" class="modal importSchemaModal" tabindex="-1" role="dialog"
        aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
        <div class="modal-dialog modal-dialog-centered" role="document">
          <!-- Modal content-->
          <div class="modal-content">
            <div class="modal-header content-center">
              <h5 class="modal-title modal-bg" id="exampleModalLongTitle">${LOAD_SESSION_FILE_TITLE}</h5>
              <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
            </div>
            <div class="modal-body">
              <form id="importForm" class="importForm">
                <div class="form-group">
                  <label class="modal-label" for="importDbType">${DB_TYPE_LABEL}</label>
                  <select class="form-control import-db-input" id="importDbType" name="importDbType">
                    <option value="" class="template"></option>
                    <option class="db-option" value="mysql">${MYSQL_DB}</option>
                    <option class="db-option" value="postgres">${POSTGRES_DB}</option>
                  </select>
                </div>
                <div class="form-group">
                  <label class="modal-label" for="sessionFilePath">${SESSION_FILE_PATH_LABEL}</label>
                  <input class="form-control load-db-input" aria-describedby="" type="text" name="sessionFilePath"
                    id="sessionFilePath" autocomplete="off"
                    onfocusout="validateInput(document.getElementById('sessionFilePath'), 'loadSessionError')" />
                  <span class='formError' id='loadSessionError'></span>
                </div>
              </form>
            </div>
            <div class="modal-footer">
              <input type='submit' disabled='disabled' id='importButton' class='connectButton' value='Confirm'
                onclick='storeSessionFilePath(document.getElementById("importDbType").value, document.getElementById("sessionFilePath").value)' />
            </div>
          </div>
        </div>
      </div>
    `
  }

  constructor() {
    super();
  }
}

window.customElements.define('hb-modal', Modal)