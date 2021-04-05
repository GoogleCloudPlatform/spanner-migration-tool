import Forms from "../../services/Forms.service.js";
import Actions from "../../services/Action.service.js";

class AddIndexForm extends HTMLElement {

     connectedCallback() {
        this.render();
        document.getElementById('createIndexButton').addEventListener('click' , () => {
            Actions.createNewSecIndex()
            // window.location.href = "#/schema-report";
            // document.getElementById('app').innerHTML = `<hb-default-layout><hb-schema-conversion-screen></hb-schema-conversion-screen></<hb-default-layout>`

        } )
    }

    render() {
        this.innerHTML = `
        <form id="createIndexForm">
        <div class="form-group sec-index-label">
            <label for="indexName" class="bmd-label-floating" style="color: black; width: 452px;">Enter
                secondary index name</label>
            <input type="text" class="form-control" name="indexName" id="indexName" autocomplete="off"
                onfocusout="validateInput(document.getElementById('indexName'), 'indexNameError')"
                style="border: 1px solid black !important;">
            <span class='form-error' id='indexNameError'></span>
        </div>
        <div class="newIndexColumnList template">
            <span class="order-id" style="visibility: hidden;">1</span><span class="columnName"></span>

            <span class="bmd-form-group is-filled">
                <div class="checkbox" style="float: right;">
                    <label>
                        <input type="checkbox" value="">
                        <span class="checkbox-decorator"><span class="check"
                                style="border: 1px solid black;"></span>
                            <div class="ripple-container"></div>
                        </span>
                    </label>
                </div>

            </span>
        </div>
        <div id="newIndexColumnListDiv" style="max-height: 200px; overflow-y: auto; overflow-x: hidden;"></div>
        <!-- <div style="display: inline-flex;">
            <div class="pmd-chip">Example Chip <a class="pmd-chip-action" href="javascript:void(0);">
                <i class="material-icons">close</i></a>
            </div>
        </div>
        <br> -->
        <div style="display: inline-flex;">
            <span style="margin-top: 18px; margin-right: 10px;">Unique</span>
            <label class="switch">
                <input id="uniqueSwitch" type="checkbox">
                <span class="slider round" id="sliderSpan"></span>
            </label>
        </div>
    </form>
        `;}

    constructor() {
        super();
    }
}

window.customElements.define('hb-add-index-form', AddIndexForm);