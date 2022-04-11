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

import '../EditGlobalDataTypeForm.componenet.js'

test('edit global data type form is rendered',()=>{
    document.body.innerHTML = '<hb-edit-global-datatype-form></hb-edit-global-datatype-form>'
    let component = document.querySelector('hb-edit-global-datatype-form');
    let table = document.getElementById("global-data-type-table");
    expect(component).not.toBe(null);
    expect(table).not.toBe(null);
})

