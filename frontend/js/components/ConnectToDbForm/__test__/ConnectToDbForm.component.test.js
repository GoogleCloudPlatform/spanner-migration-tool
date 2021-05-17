import '../ConnectToDbForm.component.js';


describe('test for connect to Db', ()=>{
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
    });

    test('Connect to db form validation', () => {
        document.body.innerHTML = `<hb-connect-to-db-form></hb-connect-to-db-form>`;
        let dbType = document.getElementById('db-type');
        let dbHost = document.getElementById('db-host');
        let dbPort = document.getElementById('db-port');
        let dbUser = document.getElementById('db-user');
        let dbName = document.getElementById('db-name');
        let dbPassword = document.getElementById('db-password');
        expect(dbType).not.toBe(null);
        expect(dbHost).not.toBe(null);
        expect(dbPort).not.toBe(null);
        expect(dbUser).not.toBe(null);
        expect(dbName).not.toBe(null);
        expect(dbPassword).not.toBe(null);
        dbType.selectedIndex = 1;
        dbHost.value = "localhost";
        dbPort.value = 3306;
        dbUser.value = "root";
        dbName.value = "sakila";
        dbPassword.value = "mysql";
        expect(dbHost.value).toBe('localhost');
        expect(dbPort.value).toBe("3306");
        expect(dbUser.value).toBe('root');
        expect(dbName.value).toBe('sakila');
        expect(dbPassword.value).toBe('mysql');
    });

})