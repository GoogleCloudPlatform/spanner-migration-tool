import '../ListTable.component.js'

describe('List table component tests',()=>{
    test('List table component should render with summary data on Dom', () => {
        document.body.innerHTML = `<hb-list-table tabName="summary" tableName="actor" dta="line1 \n line2"></hb-list-table>`
        let component = document.body.querySelector('hb-list-table');
        expect(component).not.toBe(null)
        expect(component.innerHTML).not.toBe('')
        expect(component.tableName).toBe('actor')
        expect(component.tabName).toBe('summary')
        expect(document.querySelector(`.mdc-card.${component.tabName}-content`).innerHTML.length).toEqual(59)
    });

    test('List table component should render with ddl data on Dom', () => {
        document.body.innerHTML = `<hb-list-table tabName="ddl" tableName="actor" dta=" CREATE TABLE line1 \n line2"></hb-list-table>`
        let component = document.body.querySelector('hb-list-table');
        expect(component).not.toBe(null)
        expect(component.innerHTML).not.toBe('')
        expect(component.tableName).toBe('actor')
        expect(component.tabName).toBe('ddl')
        expect(document.querySelector(`.mdc-card.${component.tabName}-content`).innerHTML).toContain('<pre><code>')
        expect(document.querySelector(`.mdc-card.${component.tabName}-content`).innerHTML).toContain('font color=\"#4285f4\"')
    });
})