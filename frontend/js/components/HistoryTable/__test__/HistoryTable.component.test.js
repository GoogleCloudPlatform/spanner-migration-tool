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

import "../HistoryTable.component.js";

describe('History table  component tests',()=>{
    afterEach(()=>{
        while(document.body.firstChild)
        {
            document.body.removeChild(document.body.firstChild)
        }
    })

    test('conditional rendering in history table',()=>{

        sessionStorage.setItem("sessionStorage",JSON.stringify([{"driver":"mysqldump","filePath":"frontend/mysqldump_2021-05-06_b878-c5ea.session.json","dbName":"mysqldump_2021-05-06_b878-c5ea","createdAt":"Thu, 06 May 2021 14:39:41 IST","sourceDbType":"MySQL"}]))
        let sessionArray = JSON.parse(sessionStorage.getItem("sessionStorage"));
        document.body.innerHTML = `<hb-history-table></hb-history-table>`
        if(sessionArray){
            let rows = document.querySelectorAll('tr.sessions');
            expect(rows.length).toBe(sessionArray.length);
        }
        else {
            let img = document.querySelector('img');
            expect(img.src).toBe("http://localhost/Icons/Icons/Group%202154.svg");
        }

    })

    test('resume handler test',()=>{
        // sessionStorage.setItem("sessionStorage",JSON.stringify([{"driver":"mysqldump","filePath":"frontend/mysqldump_2021-05-06_b878-c5ea.session.json","dbName":"mysqldump_2021-05-06_b878-c5ea","createdAt":"Thu, 06 May 2021 14:39:41 IST","sourceDbType":"MySQL"}]))
        document.body.innerHTML = `<hb-history-table></hb-history-table>`
        let resumebtn = document.querySelector('#session0');
        expect(resumebtn.className).toBe("resume-session-link");


    })
})