import '../TableCarousel.component.js';

describe('carausel component tests',()=>{
    test('Table carousel report rendering', () => {
        document.body.innerHTML = `<hb-table-carousel tabId="report" tableTitle="actor"></hb-table-carousel>`;
        let component = document.body.querySelector('hb-table-carousel');
        expect(component).not.toBe(null);
        expect(component.tabId).toBe('report');
        expect(component.tableTitle).toBe('actor');
        expect(component.innerHTML).toBe('');
    });

    test('Table carousel ddl rendering', () => {
        document.body.innerHTML = `<hb-table-carousel tabId="ddl" tableTitle="actor"></hb-table-carousel>`;
        let component = document.body.querySelector('hb-table-carousel');
        expect(component).not.toBe(null);
        expect(component.tabId).toBe('ddl');
        expect(component.tableTitle).toBe('actor');
        expect(component.innerHTML).not.toBe('');
    });

    test('Table carousel summary rendering', () => {
        document.body.innerHTML = `<hb-table-carousel tabId="summary" tableTitle="actor"></hb-table-carousel>`;
        let component = document.body.querySelector('hb-table-carousel');
        expect(component).not.toBe(null);
        expect(component.tabId).toBe('summary');
        expect(component.tableTitle).toBe('actor');
        expect(component.innerHTML).not.toBe('');
    });
})