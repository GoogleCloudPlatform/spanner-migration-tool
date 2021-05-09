import "./../MainScreen.page.js"

describe("mainscreen page render test",()=>{
    beforeAll(()=>{
        document.body.innerHTML = '<hb-main-screen></hb-main-screen>'
    })

    test('hb-label component on mainscreen rendered',()=>{
        expect(document.querySelectorAll('hb-label').length).toBe(6)
        expect(document.querySelectorAll('hb-label')[0].innerHTML).not.toBe(null)
    });

    test('hb-ui-image component rendered',()=>{
        let imageIcon = document.querySelectorAll('hb-image-icon ');
        expect(imageIcon.length).toBe(3)
        expect(imageIcon[0].label).toBe('Connect to Database')
        expect(imageIcon[0].innerHTML).not.toBe(null)

    });

    test('hb-ui-modal component on mainscreen render correctly',()=>{
        let modals = document.querySelectorAll('hb-modal');
        expect(modals.length).toBe(5)
        expect(modals[1].content).toBe('<hb-load-db-dump-form></hb-load-db-dump-form>')
        expect(modals[1].title).toBe('Load Database Dump')
        expect(modals[1].innerHTML).not.toBe(null)

    })

    test('hb-history-table component rendered correctly ',()=>{
        expect(document.querySelectorAll('hb-history-table').length).toBe(1)
        expect(document.querySelectorAll('hb-history-table')[0].innerHTML).not.toBe(null)
        
    })
})

describe('modal opening tests',()=>{

    beforeAll(()=>{
        document.body.innerHTML = '<div><hb-main-screen></hb-main-screen></div>'
        let btn = document.querySelectorAll('.image-icon')
        expect(btn.length).toBe(3)
        btn[2].click();
    })

    test('modal should open when you click hb-image-icon component',()=>{
        expect(document.getElementById('loadSchemaModal').style.display).toBe('block')
        expect(document.getElementById('loadSchemaModal').className).toBe('modal show')
    }); 
})


