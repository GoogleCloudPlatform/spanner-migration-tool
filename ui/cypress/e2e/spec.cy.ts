import mockIConv from "../../src/mocks/conv";

describe('template spec', () => {
  let url = window.location.origin;
  beforeEach(() => {
    // Intercept the backend APIs and return desired response.
    cy.intercept('GET', `${url}/IsOffline`, { statusCode: 200, body: false }).as('getIsOffline');
    cy.intercept('GET', `${url}/GetSessions`, { statusCode: 200, body: [] }).as('getSessions');
    cy.intercept('GET', `${url}/GetConfig`, { statusCode: 200 }).as('getConfig');
    cy.intercept('GET', `${url}/GetLatestSessionDetails`, { statusCode: 200 }).as('getLatestSessionDetails');
    cy.visit('http://localhost:4200/');
  });

  it('verify direct connection to mysql non-sharded database', () => {
    cy.intercept('GET', `${url}/ping`, { statusCode: 200 }).as('checkBackendHealth');
    cy.intercept('GET', `${url}/convert/infoschema`, { statusCode: 200, body: mockIConv }).as('directConnection');

    cy.get('.primary-header').eq(0).should('have.text', 'Get started with Spanner migration tool');
    cy.get('#edit-icon').should('exist').click();

    cy.fixture('config').then((json) => {
      cy.intercept('POST', `${url}/SetSpannerConfig`, (req) => {
        req.reply({
          status: 200,
          body: {
            GCPProjectID: json.projectId,
            SpannerInstanceID: json.instanceId,
            IsMetadataDbCreated: false,
            IsConfigValid: true,
          },
        });
      }).as('setSpannerConfig');
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
      cy.intercept('POST', `${url}/connect`, (req) => {
        if (req.body && req.body.Host === json.hostname && req.body.User === json.username && req.body.Driver === 'mysql'
          && req.body.Password === json.password && req.body.Port === json.port && req.body.Database === json.dbName) {
          req.reply({
            statusCode: 200,
          });
        } else {
          req.reply({
            statusCode: 400,
          });
        }
      }).as('testConnection');
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
    cy.get('#connect-btn', { timeout: 10000 }).should('be.enabled')
    cy.get('#connect-btn').click();

    // Check that workspace is rendered with correct number of tables in Object Viewer
    cy.url().should('include', '/workspace');
    cy.fixture('mysql-config').then((json) => {
      cy.get('#object-list').find('tbody tr').should('have.length', json.tableCount + 3);
    })
  });
});