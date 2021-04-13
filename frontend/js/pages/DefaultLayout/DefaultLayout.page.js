import "../../components/Header/Header.component.js";
import Actions from "../../services/Action.service.js";
import "./../../components/LoadingSpinner/LoadingSpinner.component.js"
class DefaultLayout extends HTMLElement {
    
    connectedCallback() {
        var data ; 
        data=(this.children[0])
        this.render(data);
    }
    
    render(data) {
        this.innerHTML= `
        <header class="main-header">
        <hb-header></hb-header>
        <hb-loading-spinner></hb-loading-spinner>
        </header>
        <div>${data.outerHTML}</div>`;
        Actions.hideSpinner()
    }

    constructor() {
        super();
    }

}

window.customElements.define('hb-default-layout', DefaultLayout);
