import Actions from '../../services/Action.service.js';

class ModalDb extends HTMLElement{
    
//    get modalId () {
//        return this.getAttribute('modalId');
//    }


//    get title () {
//     return this.getAttribute('title');
// }

    constructor(){
        super();
        // this.status = true;
        // this.openDb= Actions.getopenvalue();
        this.modalId= Actions.getModalId();
        
    }
     
    connectedCallback(){
        this.render();
    }


    render(){
        // let {modalId,title} = this;
        // console.log(modalId,title);
        this.innerHTML= ` <div class="modal loadDatabaseDumpModal" id="loadDatabaseDumpModal" tabindex="-1" role="dialog"
        aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
        <div class="modal-dialog modal-dialog-centered" role="document">
       
          <!-- Modal content-->
          <div class="modal-content">
            <div class="modal-header content-center">
              <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Load Database Dump</h5>
              <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
            </div>
            <div class="modal-body">
              <!-- <form id="loadDbForm"> -->
              <div class="form-group">
                <label class="" for="loadDbType">Database Type</label>
                <select class="form-control load-db-input" id="loadDbType" name="loadDbType">
                  <option value="" style="display: none;"></option>
                  <option class="db-option" value="mysql">MySQL</option>
                  <option class="db-option" value="postgres">Postgres</option>
                </select>
              </div>
              <form id="loadDbForm">
                <div class="form-group">
                  <label class="modal-label" for="dumpFilePath">Path of the Dump File</label>
                  <input class="form-control load-db-input" aria-describedby="" type="text" name="dumpFilePath"
                    id="dumpFilePath" autocomplete="off"
                    onfocusout="validateInput(document.getElementById('dumpFilePath'), 'filePathError')" />
                  <span class='formError' id='filePathError'></span>
                </div>
                <input type="text" style="display: none;">
              </form>
            </div>
            <div class="modal-footer">
              <input type="submit" disabled='disabled' value='Confirm' id='loadConnectButton' class='connectButton'
                onclick='storeDumpFileValues(document.getElementById("loadDbType").value, document.getElementById("dumpFilePath").value)' />
            </div>
          </div>
        </div>
      </div>
      

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
      </div>
      <div id="sqlFieldsButtons" style="display: none;">
        <div class="modal-footer">
          <input type="submit" disabled="disabled" value="Connect" id="connectButton" class="connectButton"
            onclick="onconnect( document.getElementById('dbType').value, document.getElementById('dbHost').value, document.getElementById('dbPort').value, document.getElementById('dbUser').value, document.getElementById('dbName').value, document.getElementById('dbPassword').value)" />
        </div>
      </div>
    </div>
  </div>
</div>


<div class="modal importSchemaModal" id="importSchemaModal" tabindex="-1" role="dialog"
  aria-labelledby="exampleModalCenterTitle" aria-hidden="true">
  <div class="modal-dialog modal-dialog-centered" role="document">
    <!-- Modal content-->
    <div class="modal-content">
      <div class="modal-header content-center">
        <h5 class="modal-title modal-bg" id="exampleModalLongTitle">Load Session File</h5>
        <i class="large material-icons close" data-dismiss="modal" onclick="clearModal()">cancel</i>
      </div>
      <div class="modal-body">
        <form id="importForm" class="importForm">
          <div class="form-group">
            <label class="modal-label" for="importDbType">Database Type</label>
            <select class="form-control import-db-input" id="importDbType" name="importDbType">
              <option value="" style="display: none;"></option>
              <option class="db-option" value="mysql">MySQL</option>
              <option class="db-option" value="postgres">Postgres</option>
            </select>
          </div>
          <div class="form-group">
            <label class="modal-label" for="sessionFilePath">Path of the session File</label>
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
}

window.customElements.define('hb-modal', ModalDb)