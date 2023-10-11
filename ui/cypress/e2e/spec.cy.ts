describe('template spec', () => {
  beforeEach(() => {
    cy.visit('http://localhost:4200/'); // Adjust the URL to match your Angular app's login page URL
    cy.fixture('config.json').as('configData');
  });

  // Test Case 1: Check if the login form elements are present
  it('verify page elements', () => {
    cy.get('.primary-header').eq(0).should('have.text', 'Get started with Spanner migration tool');

    cy.get('#edit-icon').should('exist').click();

    cy.fixture('config').then((json) => {
      cy.get('#project-id').clear().type(json.projectId)
      cy.get('#instance-id').clear().type(json.instanceId)
    })
    
    cy.get('#save-button').click();
    cy.wait(5000);
    cy.get('#check-icon', { timeout: 10000 }).should('exist');
    cy.get('#connect-to-database-btn', { timeout: 10000 }).should('exist');
    cy.get('#connect-to-database-btn').click();

    // Wait for the connection to complete (you may need to adjust the wait time)
    cy.get('#direct-connection-component', { timeout: 10000 }).should('be.visible');

    // Assuming there are input fields for database type, hostname, username, and password
    cy.get('#dbengine-input').click();
    cy.get('mat-option').contains('MySQL').click();
    cy.get('#hostname-input').type('localhost');
    cy.get('#username-input').type('root');
    cy.get('#password-input').type('pass123');
    cy.get('#port-input').type('3307');
    cy.get('#dbname-input').type('test_interleave_table_data');
    cy.get('#spanner-dialect-input').click();
    cy.get('mat-option').contains('Google Standard SQL Dialect').click();

    // Check if the button is enabled
    cy.get('#test-connect-btn').should('be.enabled').then((button) => {
      // Output the result (true if enabled, false if disabled)
      const isButtonEnabled = Cypress.$(button).is(':enabled');
      console.log('Is button enabled:', isButtonEnabled);
    });

    // Submit the form
    cy.get('#test-connect-btn').click();
    cy.get('#connect-btn').click();

    // No need to quit Cypress; it handles browser instances automatically
  });
});