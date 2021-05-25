import '../LoadSessionFileForm.component.js';

describe('load sessions component tests',()=>{
    test('Load Session File Form Rendering', () => {
        document.body.innerHTML = `<hb-load-session-file-form></hb-load-session-file-form>`;
        let component = document.body.querySelector('hb-load-session-file-form');
        expect(component).not.toBe(null);
        expect(document.getElementById('import-db-type')).not.toBe(null);
        expect(document.getElementById('session-file-path')).not.toBe(null);
    });

    test('Load Session File Form Validation', () => {
        let dbType = document.getElementById('import-db-type');
        let filePath = document.getElementById('session-file-path');
        expect(dbType).not.toBe(null);
        expect(filePath).not.toBe(null);
        dbType.selectedIndex = 1;
        filePath.value = "sagar.sql";
        expect(document.getElementById('session-file-path').value).toBe('sagar.sql');
    });
})