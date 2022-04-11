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

import "./../Tab.component.js";
import "./../../LoadingSpinner/LoadingSpinner.component.js"
import Store from "./../../../services/Store.service.js"

describe('enabled tab features',()=> {
    afterEach(() => {
        while (document.body.firstChild) {
            document.body.removeChild(document.body.firstChild)
        }
    })

    test('current tab features', () => {
        document.body.innerHTML = `<hb-tab currentTab="reportTab" ><hb-tab/>`;
        let tab = document.querySelector('#reportTab')
        expect(tab.className).toBe("nav-link active show")
        let otherTab = document.querySelector('#ddlTab')
        expect(otherTab.className).toBe("nav-link ")
    })

    test('total tabs', () => {
        document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab="reportTab"><hb-tab/></div>`;
        let tabsarray = document.querySelectorAll('li.nav-item');
        expect(tabsarray.length).toBe(3);
    })
})

describe('disabled tab features', () => {
    afterEach(() => {
        while (document.body.firstChild) {
            document.body.removeChild(document.body.firstChild)
        }
    })

    let currenttab = Store.getinstance().currentTab;
    document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab=${currenttab}><hb-tab/></div>`;
    let tab = document.querySelector('#ddlTab')
    expect(tab.className).toBe("nav-link ")

    tab.click();
    currenttab = Store.getinstance().currentTab
    document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab=${currenttab}><hb-tab/></div>`;
    tab = document.querySelector('#ddlTab')
    expect(tab.className).toBe("nav-link active show")
})