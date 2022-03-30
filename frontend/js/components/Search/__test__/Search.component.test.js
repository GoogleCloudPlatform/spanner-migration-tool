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

import '../Search.component.js'

test('Search component should render' ,()=>{
    let currentTab = "reportTab"
    document.body.innerHTML =` <hb-search tabid="${currentTab}" class="inlineblock" ></hb-search>`
    let component = document.body.querySelector('hb-search');
    expect(component).not.toBe(null)
    expect(component.innerHTML).not.toBe('')
    expect(document.querySelector('form')).not.toBe(null)
    let input = document.getElementById('search-input');
    expect(input.value).toBe('')
    input.value = "actor"
    expect(input.value).toBe('actor')
})