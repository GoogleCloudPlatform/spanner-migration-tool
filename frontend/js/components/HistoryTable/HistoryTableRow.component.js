class HistoryTableRow extends HTMLElement {
  constructor() {
    super();
}
connectedCallback() {
  // this.stateObserver = setInterval(this.observeState, 200);
  this.render();
}

disconnectedCallback() {
  // clearInterval(this.stateObserver);
}

render() {
  this.innerHTML = `
        <tr class='sessions'>
            <td class='col-2 session-table-td2 sessionName'>1</td>
            <td class='col-4 session-table-td2 sessionDate'>2</td>
            <td class='col-2 session-table-td2 sessionTime'>2</td>
            <td class='col-4 session-table-td2 session-action'>
              <a style='cursor: pointer; text-decoration: none;'>Resume Session</a>
            </td>
          </tr>
          
          `;
}
}

window.customElements.define('hb-history-table-row', HistoryTableRow);
