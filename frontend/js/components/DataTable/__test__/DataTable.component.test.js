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

import "./../DataTable.component.js";
import "./../../SiteButton/SiteButton.component.js"

describe('dataTable tests', () => {

    beforeEach(() => {
        document.body.innerHTML = `<hb-data-table tableName="test table title" tableIndex="0"></hb-data-table>`;
    })

    test('should not render if data not passed ', () => {
        let dataTable = document.querySelector("hb-data-table");
        expect(dataTable).not.toBe(null);
        expect(dataTable.innerHTML).toBe("");
    })


    test("data table component should render with given data", () => {
        let dataTable = document.querySelector("hb-data-table");
        expect(dataTable).not.toBe(null);
        expect(dataTable.innerHTML).toBe("");
        expect(dataTable.tableName).toBe('test table title');
    });
})