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

import '../TableCarousel.component.js';

describe('carausel component tests',()=>{
    test('Table carousel report rendering', () => {
        document.body.innerHTML = `<hb-table-carousel tabId="report" tableTitle="actor"></hb-table-carousel>`;
        let component = document.body.querySelector('hb-table-carousel');
        expect(component).not.toBe(null);
        expect(component.tabId).toBe('report');
        expect(component.tableTitle).toBe('actor');
        expect(component.innerHTML).toBe('');
    });

    test('Table carousel ddl rendering', () => {
        document.body.innerHTML = `<hb-table-carousel tabId="ddl" tableTitle="actor"></hb-table-carousel>`;
        let component = document.body.querySelector('hb-table-carousel');
        expect(component).not.toBe(null);
        expect(component.tabId).toBe('ddl');
        expect(component.tableTitle).toBe('actor');
        expect(component.innerHTML).not.toBe('');
    });

    test('Table carousel summary rendering', () => {
        document.body.innerHTML = `<hb-table-carousel tabId="summary" tableTitle="actor"></hb-table-carousel>`;
        let component = document.body.querySelector('hb-table-carousel');
        expect(component).not.toBe(null);
        expect(component.tabId).toBe('summary');
        expect(component.tableTitle).toBe('actor');
        expect(component.innerHTML).not.toBe('');
    });
})