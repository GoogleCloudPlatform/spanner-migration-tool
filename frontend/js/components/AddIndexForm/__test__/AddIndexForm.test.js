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

import '../AddIndexForm.component.js'

describe('rendering test of add index',()=>{
    test('Add Index form component render fine' ,()=>{
        let dummydata = []
        document.body.innerHTML =`<hb-add-index-form tableName="test table"
        tableIndex="test index" coldata=${JSON.stringify(dummydata)} ></hb-add-index-form>`
        let component = document.body.querySelector('hb-add-index-form');
        expect(component).not.toBe(null)
        expect(component.innerHTML).not.toBe('')
        expect(document.getElementById('create-index-form')).not.toBe(null)
        expect(document.getElementById('create-index-form').innerHTML).not.toBe('')
        expect(document.getElementById('create-index-form').innerHTML).not.toBe('')
        expect(component.tableName).toBe('test table')
        expect(component.tableIndex).toBe('test index')
        expect(document.getElementsByClassName('unique-swith-container')[0]).not.toBe(null)
    })


    test('render index name text box and checkbox list with dummy data',()=>{
    let dummyData = ['col1','col2','col3','col4'];
    document.body.innerHTML =`<hb-add-index-form tableName="test table"
        tableIndex="test index" coldata=${JSON.stringify(dummyData)} ></hb-add-index-form>`
        let component = document.body.querySelector('hb-add-index-form');
        expect(document.querySelectorAll('input').length).toBe(dummyData.length+2)
        let checkboxs = document.getElementsByClassName('column-name');

        for(let i=0;i<checkboxs.length;i++)
        {
            expect(document.getElementsByClassName('column-name')[i].innerHTML).toBe(dummyData[i])
            expect(document.getElementById(`checkbox-${dummyData[i]}-${i}`).checked).toBe(false)
            document.getElementById(`checkbox-${dummyData[i]}-${i}`).checked= true;
            expect(document.getElementById(`checkbox-${dummyData[i]}-${i}`).checked).toBe(true)
        }
        
    })
})

