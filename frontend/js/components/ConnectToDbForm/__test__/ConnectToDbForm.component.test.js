import '../ConnectToDbForm.component.js';

test('Connect to db form rendering', () => {
    document.body.innerHTML = `<hb-connect-to-db-form></hb-connect-to-db-form>`;
    let component = document.body.querySelector('hb-connect-to-db-form');
    expect(component).not.toBe(null);
    expect(document.getElementById('db-type')).not.toBe(null);
    document.getElementById('db-type').selectedIndex = 1;
    expect(document.getElementById('db-host')).not.toBe(null);
    expect(document.getElementById('db-port')).not.toBe(null);
    expect(document.getElementById('db-user')).not.toBe(null);
    expect(document.getElementById('db-name')).not.toBe(null);
    expect(document.getElementById('db-password')).not.toBe(null);
    expect(document.getElementById('connect-button')).not.toBe(null);
});

test('on connect button click of connect to db, modal should hide', () => {
    let dbType = document.getElementById('db-type');
    let dbHost = document.getElementById('db-host');
    let dbPort = document.getElementById('db-port');
    let dbUser = document.getElementById('db-user');
    let dbName = document.getElementById('db-name');
    let dbPassword = document.getElementById('db-password');
    let btn = document.getElementById('connect-button');
    expect(dbType).not.toBe(null);
    expect(dbHost).not.toBe(null);
    expect(dbPort).not.toBe(null);
    expect(dbUser).not.toBe(null);
    expect(dbName).not.toBe(null);
    expect(dbPassword).not.toBe(null);
    expect(btn).not.toBe(null);
    expect(btn.disabled).toBe(true);
    dbType.selectedIndex = 1;
    dbHost.value = "localhost";
    dbPort.value = 3306;
    dbUser.value = "root";
    dbName.value = "sakila";
    dbPassword.value = "mysql";
    btn.disabled = false;
    expect(btn.disabled).toBe(false);
    btn.click();
    expect(document.getElementById('connectToDbModal')).toBe(null);
});