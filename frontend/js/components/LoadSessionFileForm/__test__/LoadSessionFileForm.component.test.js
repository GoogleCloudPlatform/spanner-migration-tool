import '../LoadSessionFileForm.component.js';

test('Load Session File Form Rendering', () => {
    document.body.innerHTML = `<hb-load-session-file-form></hb-load-session-file-form>`;
    let component = document.body.querySelector('hb-load-session-file-form');
    expect(component).not.toBe(null);
    expect(document.getElementById('import-db-type')).not.toBe(null);
    expect(document.getElementById('session-file-path')).not.toBe(null);
    expect(document.getElementById('load-session-button')).not.toBe(null);
});

test('on confirm button click of load session file, modal should hide', () => {
    let dbType = document.getElementById('import-db-type');
    let filePath = document.getElementById('session-file-path');
    let btn = document.getElementById('load-session-button');
    expect(dbType).not.toBe(null)
    expect(filePath).not.toBe(null)
    expect(btn).not.toBe(null)
    expect(btn.disabled).toBe(true);
    dbType.selectedIndex = 1;
    filePath.value = "sagar.sql";
    expect(document.getElementById('session-file-path').value).toBe('sagar.sql')
    btn.disabled = false;
    expect(btn.disabled).toBe(false);
    btn.click();
    expect(document.getElementById('loadSchemaModal')).toBe(null);
});