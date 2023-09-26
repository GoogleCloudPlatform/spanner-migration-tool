// Require modules used in the logic below
const { Builder, By, Key, until, Select } = require('selenium-webdriver');

// You can use a remote Selenium Hub, but we are not doing that here
require('chromedriver');
const driver = new Builder()
    .forBrowser('chrome')
    .build();

// Setting variables for our testcase
const baseUrl = 'http://localhost:4200'

// function to check for home screen elements
var homeScreenLoadtest = async function () {

    let connectButton = By.xpath('//button');

    // navigate to the login page
    await driver.get(baseUrl);

    // wait for home page to be loaded
    await driver.wait(until.elementLocated(connectButton), 10 * 1000);
    console.log('Home screen loaded.')
}

//to set jasmine default timeout
jasmine.DEFAULT_TIMEOUT_INTERVAL = 20 * 1000;

// Start to write the first test case
describe("Selenium test case for login page", function () {
    it("verify page elements", async function () {
        console.log('<----- Starting to execute test case ----->');

        await homeScreenLoadtest();

        var welcomeMessage = By.xpath('//*[@class="primary-header"]');

        //verify welcome message on login page
        expect(await driver.findElement(welcomeMessage).getText()).toBe('Get started with Spanner migration tool');

        const connectButton = await driver.wait(
            until.elementLocated(By.id('connect-to-database-btn')),
            10000 // Adjust the timeout as needed
        );

        await connectButton.click()

        // Wait for the connection to complete (you may need to adjust the wait time)
        await driver.wait(until.elementLocated(By.id('direct-connection-component')), 10000);

        // Assuming there are input fields for database type, hostname, username, and password
        const databaseTypeInput = await driver.findElement(By.id('dbengine-input'));
        const hostnameInput = await driver.findElement(By.id('hostname-input'));
        const usernameInput = await driver.findElement(By.id('username-input'));
        const passwordInput = await driver.findElement(By.id('password-input'));
        const portInput = await driver.findElement(By.id('port-input'));
        const dbnameInput = await driver.findElement(By.id('dbname-input'));
        const testConnectionButton = await driver.findElement(By.id('test-connect-btn'));
        const spannerDialectInput = await driver.findElement(By.id('spanner-dialect-input'));

        // Fill in the form with your desired values
        await hostnameInput.sendKeys('localhost');
        await usernameInput.sendKeys('root');
        await passwordInput.sendKeys('pass123');
        await portInput.sendKeys('3307');
        await dbnameInput.sendKeys('test_interleave_table_data');

        await databaseTypeInput.click();
        optionElement = await driver.wait(
            until.elementLocated(By.xpath("//span[@class='mat-option-text'][contains(text(),'MySQL')]")),
            10000 // Adjust the timeout as needed
        );
        await optionElement.click();

        await spannerDialectInput.click();
        optionElement = await driver.wait(
            until.elementLocated(By.xpath("//span[@class='mat-option-text'][contains(text(),'Google Standard SQL Dialect')]")),
            10000 // Adjust the timeout as needed
        );
        await optionElement.click();

        // Check if the button is enabled
        const isButtonEnabled = await testConnectionButton.isEnabled();

        // Output the result (true if enabled, false if disabled)
        console.log('Is button enabled:', isButtonEnabled);

        // Submit the form
        await testConnectionButton.click();

        //to quit the web driver at end of test case execution
        
        await driver.quit();

        console.log('<----- Test case execution completed ----->');
    });
});