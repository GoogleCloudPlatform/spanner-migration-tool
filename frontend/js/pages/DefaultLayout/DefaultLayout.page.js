import "../../components/Header/Header.component.js";

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
        </header>
        <div>${data.outerHTML}</div>`;
    }

    constructor() {
        super();
    }

}

window.customElements.define('hb-default-layout', DefaultLayout);
