import Actions from '../../services/Action.service.js';
import "../../components/Label/Label.component.js";

const HISTORY_TABLE_HEADING = "Conversion history";

class HistoryTable extends HTMLElement {

  connectedCallback() {
    this.render();
    // for (let i = 0; i < this.sessionsData.length; i++) {
    //   document.getElementById(i).addEventListener('click',() => Actions.resumeSession(i))
    // }
  }

  render() {
    let sessionArray = JSON.parse(sessionStorage.getItem('sessionStorage'));
    this.innerHTML = `
        <hb-label type="text" text="${HISTORY_TABLE_HEADING}"></hb-label>
        <table class="table session-table">
        <thead>
          <tr>
            <th class='col-2 session-table-th2'>Session Name</th>
            <th class='col-4 session-table-th2'>Date</th>
            <th class='col-2 session-table-th2'>Time</th>
            <th class='col-4 session-table-th2'>Action Item</th>
          </tr>
        </thead>
        <tbody id='session-table-content'>
          ${sessionArray !== null ?
                    sessionArray.map((session, index) => {
                      let http = new XMLHttpRequest();
                      let timestampArray, sessionName, sessionDate, sessionTime;
                      timestampArray = session.createdAt.split(' ');
                      sessionName = session.filePath.split('/');
                      sessionName = sessionName[sessionName.length - 1];
                      http.open('HEAD', './' + sessionName, false);
                      http.send();
                      if (http.status !== 200) {
                        sessionArray.splice(x, 1);
                        sessionStorage.setItem('sessionStorage', JSON.stringify(sessionArray));
                      }
                      sessionDate = [timestampArray[0], timestampArray[1], timestampArray[2], timestampArray[3]].join(' ');
                      sessionTime = [timestampArray[4], timestampArray[5]].join(' ');
                      return `
                          <tr class='sessions'>
                            <td class='col-2 session-table-td2 sessionName'>${sessionName}</td>
                            <td class='col-4 session-table-td2 sessionDate'>${sessionDate}</td>
                            <td class='col-2 session-table-td2 sessionTime'>${sessionTime}</td>
                            <td class='col-4 session-table-td2 session-action'>
                              <a class="resume-session-link" id="session${index}" >Resume Session</a>
                            </td>
                          </tr>`}).join("")
            :`
              <tr class='sessionTableImg'>
                  <td colspan='5' class='center session-image'><img src='Icons/Icons/Group 2154.svg' alt='nothing to show'></td>
                </tr>
                <tr class='sessionTableNoContent'>
                  <td colspan='5' class='center simple-grey-text'>No active session available! <br> Please connect a database to
                    initiate a new session.</td>
                </tr>`
          }
        </tbody>
      </table>`;
      if (sessionArray !== null) {
        sessionArray.map(async(session, index) => {
          document.getElementById("session" + index).addEventListener('click', async () => {
            await Actions.resumeSessionHandler(index, sessionArray);
            await Actions.ddlSummaryAndConversionApiCall();
            window.location.href = '#/schema-report';
          })
        })
      }
  }

  constructor() {
    super();
  }

}

window.customElements.define('hb-history-table', HistoryTable);