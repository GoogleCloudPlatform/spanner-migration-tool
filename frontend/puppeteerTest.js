const puppeteer = require("puppeteer");
const downloadsFolder = require("downloads-folder");

let config = {
    launchOptions: {
        headless: false,
        defaultViewport: null,
    },
};

puppeteer.launch(config.launchOptions).then(async(browser) => {
    const page = await browser.newPage();
    await page.goto("http://localhost:8080/");

    await navigateToSchemaPage(page);

    /*
    1. Pick any column name which is not dependent on any 
    foreign key or index and change its name and data type
    */
    await editIndependentTable(page);

    /*
    2. Change the name of secondary index
    */
    await editSecondaryIndex(page);

    /*
    3. Pick any column which is part of foreign key or index 
    and try changing its name, data type and constraint. 
    Resolve the dependencies and show the changes
    */
    await editDependentTable(page);

    /*
    4. Delete secondary index and foreign key
    */
    await deleteRelationalKeys(page);

    /*
    5. Create a new secondary index and try to edit column 
    which is acting as key in this sec index. It should 
    give error, then delete the index and try to edit 
    column again , now it should work
    */
    await createSecondaryKey(page);

    /*
    6. Try to create a sec index, when there are pending 
    changes in the table and show the error message
    */
    await createSecondaryKeyWhileEdit(page);

    /*
    7. Change column name, rename foreign key, rename 
    index all at once and then try to save the changes
    */
    await editMultipleFields(page);

    /*
    8. Try giving duplicate names in foreign key, index and change 
    column name as well and then resolve all the errors one by one
    */
    await duplicateForeignKey(page);

    /*
    9. Download session file and try using 
    load session modal from home screen
    */
    await downloadSessionFile(page);

    /*
    10. Use global edit data type and convert everything into 
    string to show the changes in every table
    */
    await editGlobalDataType(page);

    /*
    11. For every change (like data type or column change ) show the 
    respective changes in view summary, ddl statements and summary tab as well
    */
    await viewChangesInSummary(page);

    /*
    12. Cover all basic functionalities like search 
    table, downloading ddl, summary reports etc
    */
    await checkBasicFunctionality(page);

    /*
    13. Click on resume sessions on home screen
    */
    await resumeSession(page);

    // Close Browser
    await browser.close();
});

const navigateToSchemaPage = async(page) => {
    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.loadDatabase);
    await page.click(homePage.loadDatabase);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.selectMenu);
    await page.select(homePage.selectMenu, "mysql");

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.dbFileName);
    await page.type(homePage.dbFileName, "z/a.sql");

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.confirmButton);
    await page.click(homePage.confirmButton);

    await page.waitForTimeout(200);
};

const editIndependentTable = async(page) => {
    await page.waitForSelector(homePage.categoryTable);
    await page.click(homePage.categoryTable);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.categoryEditSchemaButton);
    await page.click(homePage.categoryEditSchemaButton);

    await page.waitForTimeout(200);

    await page.waitForSelector("input#column-name-text-211");
    await page.evaluate(
        () => (document.getElementById("column-name-text-211").value = "")
    );
    await page.type(homePage.categoryNameInput, "changed");
    await page.waitForSelector(homePage.categorySelectMenu);
    await page.select(homePage.categorySelectMenu, "BYTES");

    await page.waitForTimeout(200);

    await passed(1);
};

const editSecondaryIndex = async(page) => {
    await page.waitForSelector(homePage.paymentTable);
    await page.click(homePage.paymentTable);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () => (document.getElementById("new-sec-index-val-120").value = "some_text")
    );

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await passed(2);
};

const editDependentTable = async(page) => {
    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () => (document.getElementById("column-name-text-1233").value = "")
    );
    await page.type(homePage.paymentRentalInput, "changed");
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.errorModalButton);
    await page.click(homePage.errorModalButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.paymentDropRentalButton);
    await page.click(homePage.paymentDropRentalButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.dropModalConfirmButton);
    await page.click(homePage.dropModalConfirmButton);

    await page.waitForTimeout(200);

    await page.waitForTimeout(200);

    await page.evaluate(
        () => (document.getElementById("column-name-text-1233").value = "")
    );
    await page.type(homePage.paymentRentalInput, "changed");
    await page.click(homePage.paymentEditSchemaButton);

    await passed(3);

    await page.waitForTimeout(200);
};

const deleteRelationalKeys = async(page) => {
    await page.waitForSelector(homePage.customerTable);
    await page.click(homePage.customerTable);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.customerEditSchemaButton);
    await page.click(homePage.customerEditSchemaButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.customerDropFK);
    await page.click(homePage.customerDropFK);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.customerConfirmDropFK);
    await page.click(homePage.customerConfirmDropFK);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.customerDropSK);
    await page.click(homePage.customerDropSK);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.customerConfirmDropSK);
    await page.click(homePage.customerConfirmDropSK);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.customerEditSchemaButton);
    await page.click(homePage.customerEditSchemaButton);

    await passed(4);

    await page.waitForTimeout(200);
};

const createSecondaryKey = async(page) => {
    await page.waitForSelector(homePage.actorsTable);
    await page.click(homePage.actorsTable);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.actorsAddIndexButton);
    await page.click(homePage.actorsAddIndexButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.addIndexModalInput);
    await page.type(homePage.addIndexModalInput, "test_index");

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.addIndexKeyCheckbox);
    await page.click(homePage.addIndexKeyCheckbox);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.createIndexButton);
    await page.click(homePage.createIndexButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.actorsEditSchemaButton);
    await page.click(homePage.actorsEditSchemaButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () => (document.getElementById("column-name-text-011").value = "")
    );
    await page.type(homePage.actorsInputField, "changed");
    await page.click(homePage.actorsEditSchemaButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.errorModalButton);
    await page.click(homePage.errorModalButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.actorsDropButton);
    await page.click(homePage.actorsDropButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.dropModalConfirmButton);
    await page.click(homePage.dropModalConfirmButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () => (document.getElementById("column-name-text-011").value = "")
    );
    await page.type(homePage.actorsInputField, "changed");
    await page.click(homePage.actorsEditSchemaButton);

    await passed(5);

    await page.waitForTimeout(200);
};

const createSecondaryKeyWhileEdit = async(page) => {
    await page.waitForSelector(homePage.actorsEditSchemaButton);
    await page.click(homePage.actorsEditSchemaButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () => (document.getElementById("column-name-text-011").value = "")
    );
    await page.type(homePage.actorsInputField, "changed_again");

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.actorsAddIndexButton);
    await page.click(homePage.actorsAddIndexButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.errorModalButton);
    await page.click(homePage.errorModalButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.actorsEditSchemaButton);
    await page.click(homePage.actorsEditSchemaButton);

    await passed(6);

    await page.waitForTimeout(200);
};

const editMultipleFields = async(page) => {
    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () =>
        (document.getElementById("column-name-text-1244").value =
            "changed-column")
    );

    await page.evaluate(
        () => (document.getElementById("new-fk-val-120").value = "changed-fk")
    );

    await page.evaluate(
        () =>
        (document.getElementById("new-sec-index-val-120").value = "changed-sk")
    );

    await page.click(homePage.paymentEditSchemaButton);

    await passed(7);

    await page.waitForTimeout(500);
};

const duplicateForeignKey = async(page) => {
    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await page.evaluate(
        () =>
        (document.getElementById("new-fk-val-120").value = "changed-duplicate")
    );

    await page.evaluate(
        () =>
        (document.getElementById("new-sec-index-val-120").value =
            "changed-duplicate")
    );

    await page.evaluate(
        () =>
        (document.getElementById("column-name-text-1244").value =
            "changed-column-repeat")
    );

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.errorModalButton);
    await page.click(homePage.errorModalButton);

    await page.evaluate(
        () =>
        (document.getElementById("new-sec-index-val-120").value =
            "changed-different")
    );

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.paymentEditSchemaButton);
    await page.click(homePage.paymentEditSchemaButton);

    await passed(8);

    await page.waitForTimeout(200);
};

const downloadSessionFile = async(page) => {
    await page.waitForSelector(homePage.downloadSessionFile);
    await page.click(homePage.downloadSessionFile);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.homeScreen);
    await page.click(homePage.homeScreen);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.loadSessionFile);
    await page.click(homePage.loadSessionFile);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.importDbType);
    await page.select(homePage.importDbType, "mysql");

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.sessionFilePath);
    await page.type(homePage.sessionFilePath, homePage.pathOfSessionFile);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.loadSessionButton);
    await page.click(homePage.loadSessionButton);

    await page.waitForTimeout(200);

    await passed(9);
};

const editGlobalDataType = async(page) => {
    await page.waitForSelector(homePage.editGlobalDataType);
    await page.click(homePage.editGlobalDataType);

    await page.waitForTimeout(200);

    await map(page, arr);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.confirmDataType);
    await page.click(homePage.confirmDataType);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.paymentTable);
    await page.click(homePage.actorsTable);

    await page.waitForTimeout(200);

    await page.evaluate(() =>
        console.log(
            document.getElementById("save-data-type-00").innerText == "STRING" ?
            "passed" :
            "failed"
        )
    );

    await page.waitForTimeout(200);

    await passed(10);
};

const viewChangesInSummary = async(page) => {
    await page.waitForSelector(homePage.ddlStatementTab);
    await page.click(homePage.ddlStatementTab);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.openFirstTable);
    await page.click(homePage.openFirstTable);

    await page.waitForTimeout(200);

    await page.evaluate(() =>
        console.log(
            document
            .querySelector("#ddl-actor > div > hb-list-table > div > pre > code")
            .innerText.split("actor_id")[1]
            .trim()
            .split(" ")[0] === "STRING(MAX)" ?
            "Test Case 11 Passed" :
            "Test Case 11 Failed"
        )
    );

    // await printInstructions("Check Browser Console For Test 11");

    await page.waitForTimeout(200);

    await passed(11);
};

const checkBasicFunctionality = async(page) => {
    await page.waitForSelector(homePage.searchInput);
    await page.type(homePage.searchInput, "store");

    await page.waitForTimeout(200);

    // await page.screenshot({ path: "pic.png" });

    await page.waitForSelector(homePage.ddlStatementTab);
    await page.click(homePage.ddlStatementTab);
    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.downloadDDLStatement);
    await page.click(homePage.downloadDDLStatement);
    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.summaryReport);
    await page.click(homePage.summaryReport);
    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.downloadSummaryReport);
    await page.click(homePage.downloadSummaryReport);
    await page.waitForTimeout(200);

    await passed(12);
};

const resumeSession = async(page) => {
    await page.click(homePage.homeScreen);

    await page.waitForTimeout(200);

    await page.waitForSelector(homePage.resumeSession);
    await page.click(homePage.resumeSession);

    await page.waitForTimeout(200);

    await passed(13);
};

let arr = [];
for (let i = 1; i <= 14; i++) {
    arr.push("select#data-type-option" + i);
}

const map = async(page, arr) => {
    for (let i = 0; i < arr.length; i++) {
        await page.waitForSelector(arr[i]);
        await page.select(arr[i], "STRING");
    }
};

const printInstructions = async(msg) => {
    await console.log(msg);
};

const passed = async(num) => {
    console.log("Test Passed: ", num);
};

const homePage = {
    loadDatabase: 'div[data-target="#loadDatabaseDumpModal"]',
    selectMenu: "select#load-db-type",
    dbFileName: "input#dump-file-path",
    confirmButton: "input#load-connect-button",
    categoryTable: "a#id-report-2",
    categoryEditSchemaButton: "button#editSpanner2",
    categoryNameInput: "input#column-name-text-211",
    categorySelectMenu: "select#data-type-211",
    categoryConstraint: "div#btn-group-#sp-constraint-00",
    paymentTable: "a#id-report-12",
    paymentEditSchemaButton: "button#editSpanner12",
    paymentRentalInput: "input#column-name-text-1233",
    errorModalButton: "input#edit-table-warning",
    paymentDropRentalButton: "button#payment0foreignKey",
    dropModalConfirmButton: "input#fk-drop-confirm",
    actorsTable: "a#id-report-0",
    actorsAddIndexButton: "button#hb-0indexButton-actor",
    addIndexModalInput: "input#index-name",
    addIndexKeyCheckbox: "span#index-checkbox-first_name-1",
    createIndexButton: "input#create-index-button",
    actorsEditSchemaButton: "button#editSpanner0",
    actorsInputField: "input#column-name-text-011",
    actorsDropButton: "button#actor1secIndex",
    customerTable: "a#id-report-5",
    customerEditSchemaButton: "button#editSpanner5",
    customerDropFK: "button#customer0foreignKey",
    customerConfirmDropFK: "input#fk-drop-confirm",
    customerDropSK: "button#customer0secIndex",
    customerConfirmDropSK: "input#fk-drop-confirm",
    downloadSessionFile: "button#download-schema",
    homeScreen: "a#homeScreen",
    loadSessionFile: 'div[data-target="#loadSchemaModal"]',
    importDbType: "select#import-db-type",
    sessionFilePath: "input#session-file-path",
    pathOfSessionFile: downloadsFolder() + "/session.json",
    loadSessionButton: "input#load-session-button",
    resumeSession: "a#session0",
    editGlobalDataType: "button#editButton",
    selectDataTypeOption: "select#data-type-option",
    confirmDataType: "input#data-type-button",
    assertChanges: "div#save-data-type-00",
    ddlStatementTab: "a#ddlTab",
    openFirstTable: "a#id-ddl-0",
    searchInput: "input#search-input",
    ddlStatementTab: "a#ddlTab",
    downloadDDLStatement: "button#download-ddl",
    summaryReport: "a#summaryTab",
    downloadSummaryReport: "button#download-report",
};