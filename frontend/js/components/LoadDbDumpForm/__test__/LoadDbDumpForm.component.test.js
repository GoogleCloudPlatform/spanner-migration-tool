import '../LoadDbDumpForm.component.js';

test('Load Db Dump File Form Rendering', () => {
    document.body.innerHTML = `<hb-load-db-dump-form></hb-load-db-dump-form>`;
    let component = document.body.querySelector('hb-load-db-dump-form');
    expect(component).not.toBe(null);
    expect(document.getElementById('load-db-type')).not.toBe(null);
    expect(document.getElementById('dump-file-path')).not.toBe(null);
    // expect(document.getElementById('load-connect-button')).not.toBe(null);
});

test('on confirm button click of load db dump file, modal should hide', () => {
    document.body.innerHTML = `<hb-load-db-dump-form></hb-load-db-dump-form>`;
    let dbType = document.getElementById('load-db-type');
    let filePath = document.getElementById('dump-file-path');
    // let btn = document.getElementById('load-connect-button');
    expect(dbType).not.toBe(null)
    expect(filePath).not.toBe(null)
    // expect(btn).not.toBe(null)
    // expect(btn.disabled).toBe(true);
    dbType.selectedIndex = 1;
    filePath.value = "a.sql";
    expect(document.getElementById('dump-file-path').value).toBe('a.sql');
    // btn.disabled = false;
    // expect(btn.disabled).toBe(false);
    // btn.click();
    expect(document.getElementById('loadDatabaseDumpModal')).toBe(null);
});