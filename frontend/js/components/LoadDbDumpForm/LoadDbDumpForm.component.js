class LoadDbDumpForm extends HTMLElement {

    connectedCallback() {
        this.render();
        document.getElementById('loadConnectButton').addEventListener('click', () => {this.storeDumpFileValues(document.getElementById("loadDbType").value, document.getElementById("dumpFilePath").value)})
    }

    ddlSummaryAndConversionApiCall = async () => {
        let conversionRateResp, ddlDataResp, summaryDataResp;
        await fetch('/ddl')
          .then(async function (response) {
            if (response.ok) {
              ddlDataResp = await response.json();
              localStorage.setItem('ddlStatementsContent', JSON.stringify(ddlDataResp));
              await fetch('/summary')
                .then(async function (response) {
                  if (response.ok) {
                    summaryDataResp = await response.json();
                    localStorage.setItem('summaryReportContent', JSON.stringify(summaryDataResp));
                    await fetch('/conversion')
                      .then(async function (response) {
                        if (response.ok) {
                          conversionRateResp = await response.json();
                          localStorage.setItem('tableBorderColor', JSON.stringify(conversionRateResp));
                          window.location.href = '#/schema-report';
                        }
                        else {
                          return Promise.reject(response);
                        }
                      })
                      .catch(function (err) {
                        showSnackbar(err, ' redBg');
                      });
                  }
                  else {
                    return Promise.reject(response);
                  }
                })
                .catch(function (err) {
                  showSnackbar(err, ' redBg');
                });
            }
            else {
              return Promise.reject(response);
            }
          })
          .catch(function (err) {
            showSnackbar(err, ' redBg');
          });
      }

    onLoadDatabase = async (dbType, dumpFilePath) => {
        let reportData, sourceTableFlag, reportDataResp, reportDataCopy, jsonReportDataResp, requestCode;
        reportData = await fetch('/convert/dump', {
          method: 'POST',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            "Driver": dbType,
            "Path": dumpFilePath
          })
        });
        reportDataCopy = reportData.clone();
        requestCode = reportData.status;
        reportDataResp = await reportData.text();
      
        if (requestCode != 200) {
          hideSpinner();
          showSnackbar(reportDataResp, ' redBg');
          jQuery("#loadConnectButton").attr("disabled", "disabled");
          return;
        }
        else {
          jsonReportDataResp = await reportDataCopy.json();
          if (Object.keys(jsonReportDataResp.SpSchema).length == 0) {
            showSnackbar("Please select valid file", " redBg");
            jQuery("#loadConnectButton").attr("disabled", "disabled");
            return;
          }
          else {
            // showSpinner();
            jQuery('#loadDatabaseDumpModal').modal('hide');
            localStorage.setItem('conversionReportContent', reportDataResp);
          }
      
        }
        this.ddlSummaryAndConversionApiCall();
        sourceTableFlag = localStorage.getItem('sourceDbName');
        // sessionRetrieval(sourceTableFlag);
      }

    storeDumpFileValues = (dbType, filePath) => {
        let sourceTableFlag = '';
        if (dbType === 'mysql') {
          localStorage.setItem('globalDbType', dbType + 'dump');
          sourceTableFlag = 'MySQL';
          localStorage.setItem('sourceDbName', sourceTableFlag);
        }
        else if (dbType === 'postgres') {
          localStorage.setItem('globalDbType', 'pg_dump');
          sourceTableFlag = 'Postgres';
          localStorage.setItem('sourceDbName', sourceTableFlag);
        }
        localStorage.setItem('globalDumpFilePath', filePath);
        this.onLoadDatabase(localStorage.getItem('globalDbType'), localStorage.getItem('globalDumpFilePath'));
      }

    render() {
        this.innerHTML = `
        <div class="form-group">
        <label for="loadDbType">Database Type</label>
        <select class="form-control load-db-input" id="loadDbType" name="loadDbType">
          <option value="" style="display: none;"></option>
          <option class="db-option" value="mysql">MySQL</option>
          <option class="db-option" value="postgres">Postgres</option>
        </select>
      </div>
      <form id="loadDbForm">
        <div class="form-group">
          <label class="modal-label" for="dumpFilePath">Path of the Dump File</label>
          <input class="form-control load-db-input" type="text" name="dumpFilePath"
            id="dumpFilePath" autocomplete="off"
            onfocusout="validateInput(document.getElementById('dumpFilePath'), 'filePathError')" />
          <span class='formError' id='filePathError'></span>
        </div>
        <input type="text" style="display: none;">
      </form>

      <div class="modal-footer">
        <input type="submit" value='Confirm' id='loadConnectButton' class='connectButton' />
      </div>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-load-db-dump-form', LoadDbDumpForm);