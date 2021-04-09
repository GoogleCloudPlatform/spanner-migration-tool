export const navLinks = {
    logo: {
      css: { nav: "navbar navbar-static-top", img: "logo" },
      img: { src: "../../../Icons/Icons/google-spanner-logo.png" },
    },
    links: [
      {
        text: "Home",
        href: "#/",
        aTagId: "homeScreen",
        name: "headerMenu",
      },
      {
        text: "Schema Conversion",
        href: "javascript:;",
        aTagId: "schemaScreen",
        name: "headerMenu",
      },
      {
        text: "Instructions",
        href: "#/instructions",
        aTagId: "instructions",
        name: "headerMenu",
      },
    ],
  };

export const CLASS_NAMES = {
    heading: 'heading',
    subHeading: 'sub-heading',
    text: 'text'
}

export const ModalConfigs = {

 CONNECT_TO_DB_MODAL_BUTTONS : [{ value: "Connect", id: "connect-button", disabledProp: "disabled" }] ,
 LOAD_DB_DUMP_MODAL_BUTTONS : [{ value: "Confirm", id: "load-connect-button", disabledProp: "disabled", modalDismiss: true }] ,
 LOAD_SESSION_MODAL_BUTTONS : [{ value: "Confirm", id: "load-session-button", disabledProp: "disabled", modalDismiss: true }] ,
 CONNECTION_SUCCESS_MODAL : [{ value: "Convert", id: "convert-button", disabledProp: "" }] ,
 CONNECTION_FAILURE_MODAL : [{ value: "Ok", id: "connection-failure-button", disabledProp: "" }] ,
 EDIT_GLOBAL_DATATYPE_MODAL : [{ value: "Next", id: "data-type-button", disabledProp: "" }] ,
 EDIT_TABLE_WARNING_MODAL : [{ value: "Ok", id: "edit-table-warning", disabledProp: "" }] ,
 ADD_INDEX_MODAL : [{ value: "CREATE", id: "createIndexButton", disabledProp: "disabled", modalDismiss: true }] ,
 FK_DROP_WARNING_MODAL : [{ value: "Yes", id: "fk-drop-confirm", disabledProp: "" }, {value: "No", id:"fk-drop-cancel", disabledProp: "" }] ,
}


