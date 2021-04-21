import "../../components/Header/Header.component.js";
import Actions from "../../services/Action.service.js";
import Store from "../../services/Store.service.js";
import "./../../components/LoadingSpinner/LoadingSpinner.component.js"
class DefaultLayout extends HTMLElement {
    
    connectedCallback() {
        var data ; 
        data=(this.children[0])
        this.render(data);
    }

   async refreshHandler(data){
    if(data.outerHTML ==='<hb-schema-conversion-screen></hb-schema-conversion-screen>'){
        if(Object.keys(Store.getinstance().tableData.reportTabContent).length === 0){
            let sessionArray = JSON.parse(sessionStorage.getItem("sessionStorage"));
            if(!sessionArray || sessionArray.length === 0)
            {
                window.location.href = '/';
            }
            await Actions.resumeSessionHandler(0, sessionArray);
            await Actions.ddlSummaryAndConversionApiCall();
            await Actions.setGlobalDataTypeList()
        }
    }
    }
    
    render(data) {
        this.innerHTML= `
        <header class="main-header">
        <hb-header></hb-header>
        <hb-loading-spinner></hb-loading-spinner>
        </header>
        <div>${data.outerHTML}</div>`;
        Actions.hideSpinner()
        this.refreshHandler(data)
    }

    constructor() {
        super();
    }

}

window.customElements.define('hb-default-layout', DefaultLayout);
