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
        },
        formButtonHandler: (formId, formButtonId) => {
            let formElements = document.getElementById(formId);
            formElements.querySelectorAll("input").forEach(elem => {
                elem.addEventListener("keyup", () => {
                    let empty = false;
                    formElements.querySelectorAll("input").forEach(elem => {
                        if (elem.value === '') {
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
        // clearModal: () => {
        //     document.getElementsByClassName('formError').innerHTML = '';
        //     document.getElementsByClassName('db-input').value = '';
        //     document.getElementsByClassName('db-select-input').value = '';
        //     document.getElementsByClassName('load-db-input').value = '';
        //     document.getElementsByClassName('import-db-input').value = '';
        //     document.getElementById('upload_link').innerHTML = 'Upload File';
        //     document.getElementById('loadConnectButton').disabled = true;
        //     document.getElementById('connectButton').disabled = true;
        //     document.getElementById('importButton').disabled = true;
        //     document.getElementById('indexName').value = '';
        //     document.getElementById('createIndexButton').disabled = true;
        //     if (document.getElementById('sqlFields') != undefined)
        //         document.getElementById('sqlFields').style.display = 'none';
        //     if (document.getElementById('sqlFieldsButtons') != undefined)
        //         document.getElementById('sqlFieldsButtons').style.display = 'none';
        // },
        resetConnectToDbModal: () => {
            document.getElementsByClassName('formError').innerHTML = '';
            document.getElementsByClassName('db-input').value = '';
            document.getElementById('dbType').value = '';
            document.getElementById('connectButton').disabled = true;
            if (document.getElementById('sqlFields') != undefined)
                document.getElementById('sqlFields').style.display = 'none';
            if (document.getElementById('sqlFieldsButtons') != undefined)
                document.getElementById('sqlFieldsButtons').style.display = 'none';
        },
        resetLoadDbModal: () => {
            document.getElementById('filePathError').innerHTML = '';
            document.getElementById('dumpFilePath').value = '';
            document.getElementById('loadDbType').value = '';
            document.getElementById('loadConnectButton').disabled = true;
        },
        resetLoadSessionModal: () => {
            document.getElementById('loadSessionError').innerHTML = '';
            document.getElementById('sessionFilePath').value = '';
            document.getElementById('importDbType').value = '';
            document.getElementById('importButton').disabled = true;
        }
    }
})();

export default Forms;