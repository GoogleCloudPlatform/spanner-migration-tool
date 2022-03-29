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

import Actions from "../../services/Action.service.js";
class Search extends HTMLElement {
    get tabId() {
        return this.getAttribute("tabid");
    }

    focusCampo(id) {
        var inputField = document.getElementById(id);
        if (inputField != null && inputField.value.length != 0) {
            if (inputField.createTextRange) {
                var FieldRange = inputField.createTextRange();
                FieldRange.moveStart('character', inputField.value.length);
                FieldRange.collapse();
                FieldRange.select();
            } else if (inputField.selectionStart || inputField.selectionStart == '0') {
                var elemLen = inputField.value.length;
                inputField.selectionStart = elemLen;
                inputField.selectionEnd = elemLen;
                inputField.focus();
            }
        } else {
            inputField.focus();
        }
    }

    connectedCallback() {
        this.render();
    }

    render() {

        this.innerHTML = `
        <form class="form-inline d-flex justify-content-center md-form form-sm mt-0 search-form" >
          <i class="fas fa-search" aria-hidden="true"></i>
          <input class="form-control form-control-sm ml-3 w-75 search-box" type="text" 
          placeholder="Search table" value="${Actions.getSearchInputValue(this.tabId)}" id="search-input" autocomplete='off' aria-label="Search" >
        </form>`;

        document
            .getElementById('search-input')
            .addEventListener("keyup", (e) => {
                if (e.key === "Enter" || e.target.value.length === 0) {
                    e.preventDefault()
                    Actions.SearchTable(
                        document.getElementById('search-input').value,
                        this.tabId
                    )
                }
            }
            );
        let value = Actions.getSearchInputValue(Actions.getCurrentTab())
        if (value.length > 0) {
            this.focusCampo('search-input')
        }
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-search", Search);