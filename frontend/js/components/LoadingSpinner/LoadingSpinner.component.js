class LoadingSpinner extends HTMLElement {


    connectedCallback() {
        this.render();
    }

    render() {
     this.innerHTML = `
        <div class='spinner-backdrop' id='toggle-spinner'>
            <div id="spinner"></div>
        </div>`;  
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-loading-spinner', LoadingSpinner);