import "../Modal.component.js";

describe('Modal component tests',()=>{
  test("connectToDbModal render", () => {
    document.body.innerHTML = `<hb-modal
                                  modalId="connectToDbModal"
                                  content="<hb-connect-to-db-form></hb-connect-to-db-form>"
                                  contentIcon=""
                                  connectIconClass=""
                                  modalBodyClass=""
                                  title="Connect to Database">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#connect-button")).not.toBe(null);
  });

  test("loadDatabaseDumpModal render", () => {
    document.body.innerHTML = `<hb-modal
                                  modalId="loadDatabaseDumpModal"
                                  content="<hb-load-db-dump-form></hb-load-db-dump-form>"
                                  contentIcon=""
                                  connectIconClass=""
                                  modalBodyClass=""
                                  title="Load Database Dump">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#load-connect-button")).not.toBe(null);
  });

  test("loadSchemaModal render", () => {
    document.body.innerHTML = `<hb-modal
                                  modalId="loadSchemaModal"
                                  content="<hb-load-session-file-form></hb-load-session-file-form>"
                                  contentIcon=""
                                  connectIconClass=""
                                  modalBodyClass=""
                                  title="Load Session File">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#load-session-button")).not.toBe(null);
  });

  test("connectModalSuccess render", () => {
    document.body.innerHTML = `<hb-modal
                                  modalId="connectModalSuccess"
                                  content="Please click on convert button to proceed with schema conversion"
                                  contentIcon="check_circle"
                                  connectIconClass="connect-icon-success"
                                  modalBodyClass="connection-modal-body"
                                  title="Connection Successful">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#convert-button")).not.toBe(null);
  });

  test("connectModalFailure render", () => {
    document.body.innerHTML = `<hb-modal
                                  modalId="connectModalFailure"
                                  content="Please check database configuration details and try again !!"
                                  contentIcon="cancel"
                                  connectIconClass="connect-icon-failure"
                                  modalBodyClass="connection-modal-body"
                                  title="Connection Failure">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#connection-failure-button")).not.toBe(null);
  });

  test("globalDataTypeModal render", () => {
    document.body.innerHTML = `<hb-modal 
                                  modalId="globalDataTypeModal" 
                                  content="<hb-edit-global-datatype-form></hb-edit-global-datatype-form>" 
                                  contentIcon="" 
                                  connectIconClass="" 
                                  modalBodyClass="edit-global-data-type" 
                                  title="Global Data Type Mapping">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#data-type-button")).not.toBe(null);
  });

  test("index-and-key-delete-warning render", () => {
    document.body.innerHTML = `<hb-modal 
                                  modalId="index-and-key-delete-warning" 
                                  content="" 
                                  contentIcon="warning" 
                                  connectIconClass="warning-icon" 
                                  modalBodyClass="connection-modal-body" 
                                  title="Warning">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#fk-drop-confirm")).not.toBe(null);
  });

  test("editTableWarningModal render", () => {
    document.body.innerHTML = `<hb-modal 
                                  modalId="editTableWarningModal" 
                                  content="edit table" 
                                  contentIcon="cancel" 
                                  connectIconClass="connect-icon-failure" 
                                  modalBodyClass="connection-modal-body" 
                                  title="Error Message">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#edit-table-warning")).not.toBe(null);
  });

  test("createIndexModal render", () => {
    document.body.innerHTML = `<hb-modal 
                                  modalId="createIndexModal" 
                                  content="" 
                                  contentIcon="" 
                                  connectIconClass="" 
                                  modalBodyClass="" 
                                  title="Select keys for new index">
                              </hb-modal>`;

    let modal = document.querySelector("hb-modal");
    expect(modal).not.toBe(null);
    expect(modal.innerHTML).not.toBe("");
    expect(document.querySelector(".modal-content")).not.toBe(null);
    expect(document.querySelector(".modal-header")).not.toBe(null);
    expect(document.querySelector(".modal-footer")).not.toBe(null);
    expect(document.querySelector(".modal-button")).not.toBe(null);
    expect(document.querySelector("#create-index-button")).not.toBe(null);
  });
})
