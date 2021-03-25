import Forms from "../../services/Forms.service.js";

class LoadSessionFileForm extends HTMLElement {

    connectedCallback() {
        this.render();
        let modalData = document.getElementById("loadSchemaModal");
        modalData.querySelector("i").addEventListener("click", () => {
            Forms.resetLoadSessionModal();
        });
        document.getElementById("sessionFilePath").addEventListener("focusout", () => {
            Forms.validateInput(document.getElementById('sessionFilePath'), 'loadSessionError');
        });
        Forms.formButtonHandler("importForm", "importButton");
    }

    render() {
        this.innerHTML = `
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
                            id="sessionFilePath" autocomplete="off" />
                        <span class='formError' id='loadSessionError'></span>
                    </div>
                    <input type="text" class="template" value="dummyInput">
                </form>
            </div>
            <div class="modal-footer">
                <input type='submit' disabled='disabled' id='importButton' class='connectButton' value='Confirm'
                onclick='storeSessionFilePath(document.getElementById("importDbType").value, document.getElementById("sessionFilePath").value)' />
            </div>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-load-session-file-form', LoadSessionFileForm);