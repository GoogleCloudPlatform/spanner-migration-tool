// variables initialisation
const RED = '#F44336';
var schemaConversionObj, srcTableName = [], notNullConstraint = [];

/**
 * Function to set style for selected menu
 *
 * @param {string} selectedMenuId id of selected menu
 * @return {null}
 */
const setActiveSelectedMenu = (selectedMenuId) => {
  jQuery("[name='headerMenu']:not('#"+selectedMenuId+"')").addClass('inactive');
  jQuery('#'+selectedMenuId).removeClass('inactive');
}

/**
 * Function to fetch panel border color based on conversion status
 *
 * @param {string} color
 * @return {string}
 */
const panelBorderClass = (color) => {
  var borderClass = '';
  switch (color) {
    case 'RED':
      borderClass = ' redBorderBottom';
      break;
    case 'GREEN':
      borderClass = ' greenBorderBottom';
      break;
    case 'BLUE':
      borderClass = ' blueBorderBottom';
      break;
    case 'YELLOW':
      borderClass = ' yellowBorderBottom';
      break;
  }
  return borderClass;
}

/**
 * Function to card border color based on conversion status
 *
 * @param {string} color
 * @return {string}
 */
const mdcCardBorder = (color) => {
  var cardBorderClass = '';
  switch (color) {
    case 'RED':
      cardBorderClass = ' cardRedBorder';
      break;
    case 'GREEN':
      cardBorderClass = ' cardGreenBorder';
      break;
    case 'BLUE':
      cardBorderClass = ' cardBlueBorder';
      break;
    case 'YELLOW':
      cardBorderClass = ' cardYellowBorder';
  }
  return cardBorderClass;
}

/**
 * Function to snackbar on some important actions from UI
 *
 * @param {string} message message to display in snackbar
 * @param {string} bgClass background color class for snackbar
 * @return {null}
 */
const showSnackbar = (message, bgClass) => {
  var snackbar = document.getElementById("snackbar");
  snackbar.className = "show" + bgClass;
  snackbar.innerHTML = message;
  setTimeout(function () {
    snackbar.className = snackbar.className.replace("show", "");
  }, 3000);
}

/**
 * Function to show tooltips
 *
 * @return {null}
 */
const tooltipHandler = () => {
  jQuery('[data-toggle="tooltip"]').tooltip();
}

/**
 * Function to validate if input fields ae empty.
 *
 * @param {Element} inputField Input html element like <input>..
 * @return {null}
 */
const validateInput = (inputField, errorId) => {
  field = inputField;
  if (field.value.trim() === '') {
    document.getElementById(errorId).innerHTML = `Required`;
    document.getElementById(errorId).style.color = RED;
  }
  else {
    document.getElementById(errorId).innerHTML = '';
  }
}

/**
 * Function to toggle input fields based on db type.
 *
 * @return {null}
 */
const toggleDbType = () => {
  let val = document.getElementById("dbType");
  let sourceTableFlag = '';
  if (val.value === "") {
    document.getElementById("sqlFields").style.display = "none";
    document.getElementById("sqlFieldsButtons").style.display = "none";
  }
  else if (val.value === "mysql") {
    jQuery('.formError').html('');
    jQuery('.db-input').val('');
    document.getElementById("sqlFields").style.display = "block";
    document.getElementById("sqlFieldsButtons").style.display = "block";
    sourceTableFlag = 'MySQL';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
  else if (val.value === "postgres") {
    jQuery('.formError').html('');
    jQuery('.db-input').val('');
    document.getElementById("sqlFields").style.display = "block";
    document.getElementById("sqlFieldsButtons").style.display = "block";
    sourceTableFlag = 'Postgres';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
  else if (val.value === 'dynamodb') {
    document.getElementById("sqlFields").style.display = "none";
    document.getElementById("sqlFieldsButtons").style.display = "none";
    sourceTableFlag = 'dynamoDB';
    localStorage.setItem('sourceDbName', sourceTableFlag);
  }
}

/**
 * Function to change download button and search text box based on the tab selected in edit schema screen
 *
 * @param {number} id html element id for report, ddl or summary tab
 * @return {null}
 */
const findTab = (id) => {
  switch (id) {
    case 'reportTab':
      // setting download button
      document.getElementById('download-schema').style.display = 'block';
      document.getElementById('download-ddl').style.display = 'none';
      document.getElementById('download-report').style.display = 'none';

      // setting search box
      document.getElementById('reportSearchForm').style.display = 'block';
      document.getElementById('ddlSearchForm').style.setProperty('display', 'none', 'important')
      document.getElementById('summarySearchForm').style.setProperty('display', 'none', 'important')

      tableListArea = 'accordion';
      break;
    case 'ddlTab':
      document.getElementById('download-schema').style.display = 'none';
      document.getElementById('download-ddl').style.display = 'block';
      document.getElementById('download-report').style.display = 'none';

      // setting search box
      document.getElementById('reportSearchForm').style.setProperty('display', 'none', 'important')
      document.getElementById('ddlSearchForm').style.display = 'block';
      document.getElementById('summarySearchForm').style.setProperty('display', 'none', 'important')

      tableListArea = 'ddl-accordion';
      break;
    case 'summaryTab':
      document.getElementById('download-schema').style.display = 'none';
      document.getElementById('download-ddl').style.display = 'none';
      document.getElementById('download-report').style.display = 'block';

      // setting search box
      document.getElementById('reportSearchForm').style.setProperty('display', 'none', 'important')
      document.getElementById('ddlSearchForm').style.setProperty('display', 'none', 'important')
      document.getElementById('summarySearchForm').style.display = 'block';

      tableListArea = 'summary-accordion';
      break;
  }
}

/**
 * Function to clear modal input fields.
 *
 * @return {null}
 */
const clearModal = () => {
  jQuery('.formError').html('');
  jQuery('.db-input').val('');
  jQuery('.db-select-input').val('');
  jQuery('.load-db-input').val('');
  jQuery('.import-db-input').val('');
  jQuery("#upload_link").html('Upload File');
  jQuery('#loadConnectButton').attr('disabled', 'disabled');
  jQuery('#connectButton').attr('disabled', 'disabled');
  jQuery('#importButton').attr('disabled', 'disabled');
  document.getElementById("sqlFields").style.display = "none"
  document.getElementById("sqlFieldsButtons").style.display = "none"
}


/**
 * Function to show spinner during api calls
 *
 * @return {null}
 */
const showSpinner = () => {
  toggle_spinner = document.getElementById("toggle-spinner");
  toggle_spinner.style.display = "block";
}

/**
 * Function to hide spinner after api calls
 *
 * @return {null}
 */
const hideSpinner = () => {
  toggle_spinner = document.getElementById("toggle-spinner");
  toggle_spinner.style.display = "none";
  toggle_spinner.className = toggle_spinner.className.replace("show", "");
}