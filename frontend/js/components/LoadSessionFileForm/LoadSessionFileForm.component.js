// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Forms from "../../services/Forms.service.js";
import Actions from "../../services/Action.service.js";

class LoadSessionFileForm extends HTMLElement {

    connectedCallback() {
        this.render();
        if (document.getElementById("loadSchemaModal")) {
            document.getElementById("loadSchemaModal")
            .querySelector("i")
            .addEventListener("click", () => {
                Forms.resetLoadSessionModal();
            });
        }
        document.getElementById("session-file-path").addEventListener("focusout", () => {
            Forms.validateInput(document.getElementById('session-file-path'), 'load-session-error');
        });
        Forms.formButtonHandler("load-session-form", "load-session-button");
        document.getElementById("load-session-button")?.addEventListener("click", () => {
            Actions.showSpinner()
            this.storeSessionFilePath(document.getElementById("import-db-type").value, document.getElementById("session-file-path").value);
        });
    }

    storeSessionFilePath = async (dbType, filePath) => {
        let sourceTableFlag = '', loadSessionRes, ddlSummaryApiRes;
        if (dbType === 'mysql') {
            sourceTableFlag = 'MySQL';
            Actions.setSourceDbName(sourceTableFlag)

        }
        else if (dbType === 'postgres') {
            sourceTableFlag = 'Postgres';
            Actions.setSourceDbName(sourceTableFlag)

        }
        Actions.resetReportTableData();
        loadSessionRes = await Actions.onLoadSessionFile(filePath);
        ddlSummaryApiRes = await Actions.ddlSummaryAndConversionApiCall();
        Actions.setGlobalDataTypeList();
        if (loadSessionRes && ddlSummaryApiRes) {
            window.location.href = '#/schema-report';
            Actions.sessionRetrieval(Actions.getSourceDbName());
        }
        else {
            Actions.hideSpinner()
        }
    }

    render() {
        this.innerHTML = `
                <form id="load-session-form" class="load-session-form">
                    <div>
                        <label for="import-db-type">Database Type</label>
                        <select class="form-control import-db-input" id="import-db-type" name="import-db-type">
                            <option class="template"></option>
                            <option value="mysql">MySQL</option>
                            <option value="postgres">Postgres</option>
                        </select>
                    </div>
                    <div>
                        <label for="session-file-path">Path of the session File</label>
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