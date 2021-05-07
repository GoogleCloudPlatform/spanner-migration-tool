import '../TableCarousel.component.js';

test('Table carousel report rendering', () => {
    document.body.innerHTML = `<hb-table-carousel carouselClass="report-section" tableTitle="actor"></hb-table-carousel>`;
    let component = document.body.querySelector('hb-table-carousel');
    expect(component).not.toBe(null);
    expect(component.carouselClass).toBe('report-section');
    expect(component.tableTitle).toBe('actor');
    expect(component.innerHTML).not.toBe('');
});

test('Table carousel ddl rendering', () => {
    document.body.innerHTML = `<hb-table-carousel carouselClass="ddl-section" tableTitle="actor"></hb-table-carousel>`;
    let component = document.body.querySelector('hb-table-carousel');
    expect(component).not.toBe(null);
    expect(component.carouselClass).toBe('ddl-section');
    expect(component.tableTitle).toBe('actor');
    expect(component.innerHTML).not.toBe('');
});

test('Table carousel summary rendering', () => {
    document.body.innerHTML = `<hb-table-carousel carouselClass="summary-section" tableTitle="actor"></hb-table-carousel>`;
    let component = document.body.querySelector('hb-table-carousel');
    expect(component).not.toBe(null);
    expect(component.carouselClass).toBe('summary-section');
    expect(component.tableTitle).toBe('actor');
    expect(component.innerHTML).not.toBe('');
});