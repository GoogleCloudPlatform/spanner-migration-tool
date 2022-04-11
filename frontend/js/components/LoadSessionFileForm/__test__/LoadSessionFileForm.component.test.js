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

import '../LoadSessionFileForm.component.js';

describe('load sessions component tests',()=>{
    test('Load Session File Form Rendering', () => {
        document.body.innerHTML = `<hb-load-session-file-form></hb-load-session-file-form>`;
        let component = document.body.querySelector('hb-load-session-file-form');
        expect(component).not.toBe(null);
        expect(document.getElementById('import-db-type')).not.toBe(null);
        expect(document.getElementById('session-file-path')).not.toBe(null);
    });

    test('Load Session File Form Validation', () => {
        let dbType = document.getElementById('import-db-type');
        let filePath = document.getElementById('session-file-path');
        expect(dbType).not.toBe(null);
        expect(filePath).not.toBe(null);
        dbType.selectedIndex = 1;
        filePath.value = "sagar.sql";
        expect(document.getElementById('session-file-path').value).toBe('sagar.sql');
    });
})