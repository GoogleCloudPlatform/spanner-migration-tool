// Components 
import '../../components/Tab/Tab.component.js';
import "../../components/Tab/Tabb.component.js";

// Services
import Store from "./../../services/Store.service.js";

class MainScreen extends HTMLElement {
    connectedCallback() {
        this.stateObserver = setInterval(this.observeState, 200);
        this.render();
    }
    
    disconnectedCallback() {
        clearInterval(this.stateObserver);
    }

    observeState = () => {
        if(JSON.stringify(Store.getinstance()) !== JSON.stringify(this.data)) {
            this.data = Store.getinstance();
            this.render();
        } 
    }
    
    render() {
        let { open, funcc, something } = this.data;
        this.innerHTML = `
            <div>This is the new update</div>
            <hb-tab open="${open}" relay=${funcc}></hb-tab>
            <hb-tabb something="${something}" open="${open}" clickAction="addAttrToStore"></hb-tabb>
        `;
    }

    constructor() {
        super();
        this.data = Store.getinstance();
        this.stateObserver = null;
    }
}

window.customElements.define('hb-main-screen', MainScreen);
