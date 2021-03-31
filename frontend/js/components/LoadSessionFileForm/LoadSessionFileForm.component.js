import Forms from "../../services/Forms.service.js";

class LoadSessionFileForm extends HTMLElement {

    connectedCallback() {
        this.render();
        let modalData = document.getElementById("loadSchemaModal");
        modalData.querySelector("i").addEventListener("click", () => {
            Forms.resetLoadSessionModal();
        });
        document.getElementById("session-file-path").addEventListener("focusout", () => {
            Forms.validateInput(document.getElementById('session-file-path'), 'load-session-error');
        });
        Forms.formButtonHandler("load-session-form", "import-button");
        document.getElementById("import-button").addEventListener("click", () => {
            storeSessionFilePath(document.getElementById("import-db-type").value, document.getElementById("session-file-path").value)
        })
    }

    render() {
        this.innerHTML = `
                <form id="load-session-form" class="load-session-form">
                    <div>
                        <label class="modal-label" for="import-db-type">Database Type</label>
                        <select class="form-control import-db-input" id="import-db-type" name="import-db-type">
                            <option class="template"></option>
                            <option value="mysql">MySQL</option>
                            <option value="postgres">Postgres</option>
                        </select>
                    </div>
                    <div>
                        <label class="modal-label" for="session-file-path">Path of the session File</label>
                        <input class="form-control load-db-input" type="text" name="session-file-path"
                            id="session-file-path" autocomplete="off" />
                        <span class='form-error' id='load-session-error'></span>
                    </div>
                    <input type="text" class="template" value="dummyInput">
                </form>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-load-session-file-form', LoadSessionFileForm);