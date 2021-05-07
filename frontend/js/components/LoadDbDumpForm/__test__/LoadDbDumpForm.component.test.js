import '../LoadDbDumpForm.component.js';

test('Load Db Dump File Form Rendering', () => {
    document.body.innerHTML = `<hb-load-db-dump-form></hb-load-db-dump-form>`;
    let component = document.body.querySelector('hb-load-db-dump-form');
    expect(component).not.toBe(null);
    expect(document.getElementById('load-db-type')).not.toBe(null);
    expect(document.getElementById('dump-file-path')).not.toBe(null);
});

test('Load Db Dump File Form Validation', () => {
    let dbType = document.getElementById('load-db-type');
    let filePath = document.getElementById('dump-file-path');
    expect(dbType).not.toBe(null);
    expect(filePath).not.toBe(null);
    dbType.selectedIndex = 1;
    filePath.value = "a.sql";
    expect(document.getElementById('dump-file-path').value).toBe('a.sql');
});