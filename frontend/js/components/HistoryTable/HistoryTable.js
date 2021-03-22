// import '../../components/HistoryTable/HistoryTableRow.js'
import Actions from '../../services/Action.service.js';



class HistoryTable extends HTMLElement {

    // get modalDataTarget() {
    //     return this.getAttribute('modalDataTarget');
    // }
     constructor() {
        super();
        this.sessionsData = Actions.getAllSessions();
       
        console.log(this.sessionsData);
        // this.addEventListener('click', Actions[this.clickAction]);
    }

    connectedCallback() {
        this.render();
        for(let i=0;i<this.sessionsData.length;i++)
        {
          document.getElementById(`${i}`).addEventListener('click',function(){ Actions.resumeSession(i)});
        }
    }

    render() {
        // let { image, label, imageAltText, modalDataTarget } = this;
        this.innerHTML = `
        <table class="table session-table" style="width: 95%;">
        <thead>
          <tr>
            <th class='col-2 session-table-th2'>Session Name</th>
            <th class='col-4 session-table-th2'>Date</th>
            <th class='col-2 session-table-th2'>Time</th>
            <th class='col-4 session-table-th2'>Action Item</th>
          </tr>
        </thead>
        <tbody id='session-table-content'>
          ${ this.sessionsData.length>0 ?
            (
              // "<hb-history-table-row></hb-history-table-row>"
              this.sessionsData.map((item,index)=>{
                return `<tr class='sessions'>
                <td class='col-2 session-table-td2 sessionName'>${item.sessionName}</td>
                <td class='col-4 session-table-td2 sessionDate'>${item.sessionDate}</td>
                <td class='col-2 session-table-td2 sessionTime'>${item.sessionTime}</td>
                <td class='col-4 session-table-td2 session-action'>
                  <a style='cursor: pointer; text-decoration: none; ' id="${index}">Resume Session</a>
                </td>
              </tr> `;
              }
              
              )
            ) :
          (`<tr class='sessionTableImg'>
            <td colspan='5' class='center session-image'><img src='Icons/Icons/Group 2154.svg' alt='nothing to show'></td>
          </tr>
          <tr class='sessionTableNoContent'>
            <td colspan='5' class='center simple-grey-text'>No active session available! <br> Please connect a database to
              initiate a new session.</td>
          </tr>`)
          }
          
        </tbody>
      </table>
        `;
    }

   

}

window.customElements.define('hb-history-table', HistoryTable);



