import Actions from "../../services/Action.service.js";
import "../../components/Label/Label.component.js";
import {HISTORY_TABLE_HEADING} from "./../../config/constantData.js";

class HistoryTable extends HTMLElement {
  
  connectedCallback() {
    this.render();
  }

  render() {
    let sessionArray = JSON.parse(sessionStorage.getItem("sessionStorage"));
    this.innerHTML = `
        <hb-label type="sessionHeading" text="${HISTORY_TABLE_HEADING}"></hb-label>
        <table class="table session-table">
              <thead>
                <tr id="first-title-row">
                  <th class='col-2'>Session Name</th>
                  <th class='col-4'>Date</th>
                  <th class='col-2'>Time</th>
                  <th class='col-4'>Action Item</th>
                </tr>
              </thead>
              <tbody id='session-table-content'>
                ${
                  sessionArray!==null && sessionArray.length > 0
                    ? sessionArray
                        .map((session, index) => {
                          let timestampArray, sessionName, sessionDate, sessionTime;
                          timestampArray = session.createdAt.split(" ");
                          sessionName = session.filePath.split("/");
                          sessionName = sessionName[sessionName.length - 1];
                          sessionDate = [
                            timestampArray[0],
                            timestampArray[1],
                            timestampArray[2],
                            timestampArray[3],
                          ].join(" ");
                          sessionTime = [timestampArray[4], timestampArray[5]].join(
                            " "
                          );
                          return `
                                <tr class='sessions'>
                                  <td class='col-2 session-table-td2 session-dame'>${sessionName}</td>
                                  <td class='col-4 session-table-td2 session-date'>${sessionDate}</td>
                                  <td class='col-2 session-table-td2 session-time'>${sessionTime}</td>
                                  <td class='col-4 session-table-td2 session-action'>
                                    <a class="resume-session-link" id="session${index}" >Resume Session</a>
                                  </td>
                                </tr>`;
                        })
                        .join("")
                    : `
                      <tr class='session-table-img'>
                        <td colspan='5' class='center session-image'><img src='Icons/Icons/Group 2154.svg' alt='nothing to show'></td>
                      </tr>
                      <tr class='session-table-no-Content'>
                        <td colspan='5' class='center simple-grey-text'>No active session available! <br> Please connect a database to
                          initiate a new session.</td>
                      </tr>`
                }
              </tbody>
        </table>`;
    if (sessionArray !== null) {
      sessionArray.map(async (session, index) => {
        document
          .getElementById("session" + index)
          .addEventListener("click", async () => {
            Actions.resetReportTableData();
            await Actions.resumeSessionHandler(index, sessionArray);
            await Actions.ddlSummaryAndConversionApiCall();
            await Actions.setGlobalDataTypeList()
            window.location.href = "#/schema-report";
          });
      });
    }
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-history-table", HistoryTable);
