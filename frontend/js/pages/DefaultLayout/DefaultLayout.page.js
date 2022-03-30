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

import "../../components/Header/Header.component.js";
import Actions from "../../services/Action.service.js";
import Store from "../../services/Store.service.js";
import "./../../components/LoadingSpinner/LoadingSpinner.component.js"
class DefaultLayout extends HTMLElement {

    connectedCallback() {
        var data;
        data = (this.children[0])
        this.render(data);
    }

    async refreshHandler(data) {
        if (data.outerHTML === '<hb-schema-conversion-screen></hb-schema-conversion-screen>') {
            if (Object.keys(Store.getinstance().tableData.reportTabContent).length === 0) {
                let sessionArray = JSON.parse(sessionStorage.getItem("sessionStorage"));
                let idx = sessionStorage.getItem('currentSessionIdx')
                if (!sessionArray || sessionArray.length === 0 || idx === null) {
                    window.location.href = '/';
                }
                await Actions.resumeSessionHandler(idx, sessionArray);
                await Actions.ddlSummaryAndConversionApiCall();
                await Actions.setGlobalDataTypeList()
            }
        }
    }

    render(data) {
        this.innerHTML = `
        <header class="main-header">
        <hb-header></hb-header>
        <hb-loading-spinner></hb-loading-spinner>
        </header>
        <div>${data.outerHTML}</div>`;
        Actions.hideSpinner()
        this.refreshHandler(data)
        window.scrollTo(0, 0)
    }

    constructor() {
        super();
    }

}

window.customElements.define('hb-default-layout', DefaultLayout);
