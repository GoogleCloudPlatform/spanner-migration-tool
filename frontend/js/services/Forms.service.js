const RED = '#F44336';
/**
 * All the form validations are mentioned in this module
 * 
 */
const Forms = (() => {
    return {
        validateInput: (inputField, errorId) => {
            let field = inputField;
            if (field.value.trim() === '') {
                document.getElementById(errorId).innerHTML = `Required`;
                document.getElementById(errorId).style.color = RED;
            }
            else {
                document.getElementById(errorId).innerHTML = '';
            }
        },
        toggleDbType: () => {
            let val = document.getElementById("db-type");
            let sourceTableFlag = '';
            if (val.value === "") {
                document.getElementById("sql-fields").style.display = "none";
            }
            else if (val.value === "mysql") {
                jQuery('.form-error').html('');
                jQuery('.db-input').val('');
                document.getElementById("sql-fields").style.display = "block";
                sourceTableFlag = 'MySQL';
                localStorage.setItem('sourceDbName', sourceTableFlag);
            }
            else if (val.value === "postgres") {
                jQuery('.form-error').html('');
                jQuery('.db-input').val('');
                document.getElementById("sql-fields").style.display = "block";
                sourceTableFlag = 'Postgres';
                localStorage.setItem('sourceDbName', sourceTableFlag);
            }
            else if (val.value === 'dynamodb') {
                document.getElementById("sql-fields").style.display = "none";
                sourceTableFlag = 'dynamoDB';
                localStorage.setItem('sourceDbName', sourceTableFlag);
            }
        },
        formButtonHandler: (formId, formButtonId) => {
            let formElements = document.getElementById(formId);
            // console.log(formElements);
            formElements.querySelectorAll("input").forEach(elem => {
                elem.addEventListener("keyup", () => {
                    let empty = false;
                    formElements.querySelectorAll('input:not([type="checkbox"])').forEach(elem => {
                        console.log(elem.value)
                        if (elem.value === '') {
                            console.log(elem);
                            empty = true;
                        }
                        
                    });
                    if (empty) {
                        document.getElementById(formButtonId).disabled = true;
                    }
                    else {
                        document.getElementById(formButtonId).disabled = false;
                    }
                });
            });
        },
        resetConnectToDbModal: () => {
            document.getElementsByClassName('form-error').innerHTML = '';
            document.getElementsByClassName('db-input').value = '';
            document.getElementById('db-type').value = '';
            document.getElementById('connect-button').disabled = true;
            if (document.getElementById('sql-fields') != undefined)
                document.getElementById('sql-fields').style.display = 'none';
        },
        resetLoadDbModal: () => {
            document.getElementById('file-path-error').innerHTML = '';
            document.getElementById('dump-file-path').value = '';
            document.getElementById('load-db-type').value = '';
            document.getElementById('load-connect-button').disabled = true;
        },
        resetLoadSessionModal: () => {
            document.getElementById('load-session-error').innerHTML = '';
            document.getElementById('session-file-path').value = '';
            document.getElementById('import-db-type').value = '';
            document.getElementById('import-button').disabled = true;
        }
    }
})();

export default Forms;