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

import '../SiteButton.component.js'

describe('button component tests',()=>{
    afterEach(()=>{
        while(document.body.firstChild)
        {
            document.body.removeChild(document.body.firstChild)
        }
    })
    test('button component rendering and event listener added' ,()=>{
        document.body.innerHTML = `<hb-site-button buttonid="test-id" classname="test-class" 
        buttonaction="test-action" text="test Button"></hb-site-button>`
        let p = document.querySelector('hb-site-button');
        let mockFn = jest.fn((e)=>p.add(5,6))

        expect(p.innerHTML).not.toBe(null)
        expect(p.text).toBe('test Button')
        expect(p.buttonAction).toBe('test-action')
        expect(p.className).toBe('test-class')
        expect(p.buttonId).toBe('test-id')
        p.addEventListener('click',mockFn)
        p.click()
        expect(mockFn.mock.results[0].value).toBe(11)
    })

    test('add button',()=>{
        document.body.innerHTML = `<hb-site-button buttonid="test-id" classname="test-class" 
        buttonaction="test-action" text="test Button"></hb-site-button>`
        let p = document.querySelector('hb-site-button');
        expect(p.add(2,2)).toBe(4)
    })

})

