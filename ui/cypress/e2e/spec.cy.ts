describe('template spec', () => {
  beforeEach(() => {
    cy.visit('http://localhost:4200/');
    cy.fixture('config.json').as('configData');
  });

  it('verify direct connection to mysql non-sharded database', () => {
    cy.get('.primary-header').eq(0).should('have.text', 'Get started with Spanner migration tool');

    cy.get('#edit-icon').should('exist').click();

    cy.fixture('config').then((json) => {
      cy.get('#project-id').clear().type(json.projectId)
      cy.get('#instance-id').clear().type(json.instanceId)
    })

    cy.get('#save-button').click();
    cy.get('#save-button', { timeout: 50000 }).should("not.exist");
    cy.get('#check-icon').should('exist');
    cy.get('#connect-to-database-btn', { timeout: 10000 }).should('exist');
    cy.get('#connect-to-database-btn').click();

    // Wait for the connection to complete
    cy.get('#direct-connection-component', { timeout: 10000 }).should('be.visible');

    cy.get('#dbengine-input').click();
    cy.get('mat-option').contains('MySQL').click();

    cy.fixture('mysql-config').then((json) => {
      cy.get('#hostname-input').clear().type(json.hostname)
      cy.get('#username-input').clear().type(json.username)
      cy.get('#password-input').clear().type(json.password)
      cy.get('#port-input').clear().type(json.port)
      cy.get('#dbname-input').clear().type(json.dbName)
    })

    cy.get('#spanner-dialect-input').click();
    cy.fixture('constants').then((json) => {
      cy.get('mat-option').contains(json.googleDialect).click();
    })

    // Check if the button is enabled
    cy.get('#test-connect-btn').should('be.enabled')

    // Submit the form
    cy.get('#test-connect-btn').click();
    cy.get('#connect-btn').click();

    // Check that workspace is rendered with 2 tables in Object Viewer
    cy.url().should('include', '/workspace');
    cy.fixture('mysql-config').then((json) => {
      cy.get('#table-and-index-list').find('tbody tr').should('have.length', json.tableCount + 2);
    })
  });
});