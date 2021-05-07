class TabLink extends HTMLElement {
  
    get tabId() {
      return this.getAttribute("tabid");
    }
  
    get text() {
      return this.getAttribute("text");
    }
  
    get open(){
      return this.getAttribute("open")
    }

    connectedCallback() {
        this.render();
    }

    render() {

    }

    constructor() {
        super();

    }
}