import Actions from "../../services/Action.service.js";

class Modal extends HTMLElement {

    get id() {
        return this.getAttribute('id');
    }
    get isClosable() {
        return this.getAttribute('isClosable');
    }
    get title() {
        return this.getAttribute('title');
    }
    get content() {
        return this.getAttribute('content');
    }
    get show() {
        return this.getAttribute('show');
    }

    static get observedAttributes() {
        return ['show'];
    }

    attributeChangedCallback(name, oldValue, newValue) {
        console.log('MOdal updated ---- ', name, newValue);
        if (name === 'show') { this.show = newValue; }
        this.render();
    }

    connectedCallback() {
        this.render();
    }

    render() {
        let { id, isClosable, title, content, show } = this;
        if (!show) { this.innerHTML = `` } else {
            this.innerHTML = `
                <div class="modal-container" id="${id}">
                    <div class="title-bar">
                        <div class="title">${title}</div>
                        ${isClosable && `<div class="close_button">X</div>`}
                    </div>
                    <div class="content">
                        ${content}
                    </div>
                </div>
            `;
        }
    }

    constructor() {
        super();
        this.show = 'no';
        this.addEventListener('click', () => {Actions['closeModal'](id)});
    }
}

window.customElements.define('hb-image-icon', ImageIcon);
