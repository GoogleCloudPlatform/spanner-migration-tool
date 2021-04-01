import Actions from "../../services/Action.service.js";

class SiteButton extends HTMLElement {

    get buttonId() {
        return this.getAttribute("buttonid");
    }

    get text() {
        return this.getAttribute("text");
    }

    get className(){
        return this.getAttribute('classname')
    }

    get buttonAction(){
       return this.getAttribute('buttonaction')
    }

    connectedCallback() {
        this.render(); 
    }

    render() {
       
        this.innerHTML = `<button class="${this.className}" id="${this.buttonId}" >${this.text}</button>`;
    }

    constructor() {
        super();
        this.addEventListener("click", () => {
            switch(this.buttonAction){
                case "expandAll":
                  return Actions[this.buttonAction](document.getElementById(this.buttonId).innerHTML, this.buttonId)

                case "downloadSession":
                   return  Actions[this.buttonAction]()
                
                case "downloadDdl":
                    return Actions[this.buttonAction]()
                
                case "downloadReport":
                    return Actions[this.buttonAction]()

                case "editGlobalDataType":
                    return Actions[this.buttonAction]()
            }
            
       
    }
    )}
    
}

window.customElements.define("hb-site-button", SiteButton);
