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

import '../Header.component.js'


describe('header component tests',()=>{
    afterEach(()=>{
        while(document.body.firstChild)
        {
            document.body.removeChild(document.body.firstChild)
        }
    })

    test('check number of navs in header',()=>{
        document.body.innerHTML = `<hb-header></hb-header>`;
        let headerNavs = document.querySelectorAll('nav');
        expect(headerNavs.length).toBe(4);
    })

    test('check active button with blue color',()=>{
        document.body.innerHTML = `<hb-header></hb-header>`;
        let btn = document.getElementById('homeScreen');
        // expect(btn.style.color).toBe("blue")
        expect(btn.classList[0]).toBe("inactive")
        btn.click();
        // expect(btn.classList[0]).toBe("active")
    })

    test('function being called',()=>{
        document.body.innerHTML = `<hb-header></hb-header>`;
        let btn = document.getElementById('schemaScreen');
        sessionStorage.setItem("sessionStorage",JSON.stringify([{"driver":"mysqldump","filePath":"frontend/mysqldump_2021-05-06_b878-c5ea.session.json","dbName":"mysqldump_2021-05-06_b878-c5ea","createdAt":"Thu, 06 May 2021 14:39:41 IST","sourceDbType":"MySQL"}]))
        btn.click()
        
        // expect(windows.location.href).toBe("#/schema-report")
        // expect(btn.style.color).toBe("blue");
    })

    test('routing check of none',()=>{
        document.body.innerHTML = `<hb-header></hb-header>`;
        let btn = document.getElementById('schemaScreen');
        btn.click()
        

        // expect(btn.style.color).toBe("blue");
    })
})