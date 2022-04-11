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

import '../LoadDbDumpForm.component.js';

test('Load Db Dump File Form Rendering', () => {
    document.body.innerHTML = `<hb-load-db-dump-form></hb-load-db-dump-form>`;
    let component = document.body.querySelector('hb-load-db-dump-form');
    expect(component).not.toBe(null);
    expect(document.getElementById('load-db-type')).not.toBe(null);
    expect(document.getElementById('dump-file-path')).not.toBe(null);
});

test('Load Db Dump File Form Validation', () => {
    let dbType = document.getElementById('load-db-type');
    let filePath = document.getElementById('dump-file-path');
    expect(dbType).not.toBe(null);
    expect(filePath).not.toBe(null);
    dbType.selectedIndex = 1;
    filePath.value = "a.sql";
    expect(document.getElementById('dump-file-path').value).toBe('a.sql');
});