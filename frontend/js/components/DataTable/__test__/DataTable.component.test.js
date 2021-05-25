import "./../DataTable.component.js";
import "./../../SiteButton/SiteButton.component.js"

describe('dataTable tests', () => {

    beforeEach(() => {
        document.body.innerHTML = `<hb-data-table tableName="test table title" tableIndex="0"></hb-data-table>`;
    })

    test('should not render if data not passed ', () => {
        let dataTable = document.querySelector("hb-data-table");
        expect(dataTable).not.toBe(null);
        expect(dataTable.innerHTML).toBe("");
    })


    test("data table component should render with given data", () => {
        let dataTable = document.querySelector("hb-data-table");
        expect(dataTable).not.toBe(null);
        expect(dataTable.innerHTML).toBe("");
        expect(dataTable.tableName).toBe('test table title');
    });
})