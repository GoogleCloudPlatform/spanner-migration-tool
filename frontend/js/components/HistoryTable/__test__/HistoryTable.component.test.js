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