import Forms from "../../services/Forms.service.js";
import Actions from "../../services/Action.service.js";

class AddIndexForm extends HTMLElement {

    get tableName(){
        return this.getAttribute('tableName')
    }

     connectedCallback() {
        const {SrcSchema} = JSON.parse(localStorage.getItem('conversionReportContent'));
        this.render();

        document.getElementById("indexName").addEventListener("focusout",()=>{
            Forms.validateInput(document.getElementById("indexName" ),'indexNameError');
        })

        Forms.formButtonHandler("createIndexForm","createIndexButton");

        document.getElementById("createIndexModal").querySelector("i").addEventListener("click",()=>{Actions.closeSecIndexModal()})
        
        document.getElementById("createIndexButton").addEventListener("click",()=>{Actions.fetchIndexFormValues(this.tableName,document.getElementById("indexName").value,document.getElementById("uniqueSwitch").checked)})

        SrcSchema[this.tableName].ColNames.map((row,idx )=>{
            
            document.getElementById('checkbox-'+row+"-"+idx).addEventListener('click',()=>{
                Actions.changeCheckBox(row ,idx)
            })
        })


    }

    render() {
        const {SrcSchema} = JSON.parse(localStorage.getItem('conversionReportContent'));
        this.innerHTML = `
        <form id="createIndexForm">
        <div class="form-group sec-index-label">
            <label for="indexName" class="bmd-label-floating" style="color: black; width: 452px;">Enter
                secondary index name</label>
            <input type="text" class="form-control" name="indexName" id="indexName" autocomplete="off"
                style="border: 1px solid black !important;">
            <span class='form-error' id='indexNameError'></span>
        </div>
        
        <div id="newIndexColumnListDiv" style="max-height: 200px; overflow-y: auto; overflow-x: hidden;">
              ${ SrcSchema[this.tableName].ColNames.map((row,idx )=>{
                  return `
                  <div class="newIndexColumnList" id="indexColumnRow${idx}">
                    <span class="orderId"style="visibility: hidden;" id="order${row}${idx}">1</span>
                    <span class="columnName">${row}</span>
                    <span class="bmd-form-group is-filled">
                        <div class="checkbox" style="float: right;">
                            <label>
                                <input type="checkbox" value="" id="checkbox-${row}-${idx}">
                                <span class="checkbox-decorator"><span class="check" style="border: 1px solid black;"></span>
                                    <div class="ripple-container"></div>
                                </span>
                            </label>
                        </div>
                    </span>
                </div>
                  `
              }).join("")}  
    </div>
        <div style="display: inline-flex;">
            <span style="margin-top: 18px; margin-right: 10px;">Unique</span>
            <label class="switch">
                <input id="uniqueSwitch" type="checkbox">
                <span class="slider round" id="sliderSpan"></span>
            </label>
        </div>
    </form>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-add-index-form', AddIndexForm);

