import "../../components/Header/Header.js";

class DefaultLayout extends HTMLElement {
    
    connectedCallback() {
        var data ; 
        data=(this.children[0])
        console.log(data);
        this.render(data);
       
    }
    
    render(data) {
        console.log(data)
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
