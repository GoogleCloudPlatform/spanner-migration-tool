// import Actions from "./../../services/Action.service.js";

// class Tabb extends HTMLElement {
//     connectedCallback() {
//         // this.render();
//     }

//     static get observedAttributes() {
//         return ['open'];
//     }

//     attributeChangedCallback(name, oldValue, newValue) {
//         console.log('in the attr change -Tabbnnn', name, oldValue, newValue);
//         if (name === 'open') { this.openValue === newValue }
//         console.log(this.openValue);
//         this.render();
//     }

//     clickHandler() {
//         Actions[this.clickAction]();
//         this.render();
//     }

//     get open() {
//         return this.getAttribute('open');
//     }

//     get something() {
//         return this.getAttribute('something');
//     }

//     get clickAction() {
//         return this.getAttribute('clickAction');
//     }

//     render() {
//         let { openValue, something } = this;
//         console.log(openValue, something, ' are the values ');
//         this.innerHTML = `
//             <div>
//                 <div>This is the Tabb component - ${openValue}</div>
//                 <div>Value os something is ${something}</div>
//             </div>
//         `;
//     }

//     constructor() {
//         super();
//         this.openValue = "";
//         this.addEventListener('click', this.clickHandler); // Actions[this.clickAction]);
//     }
// }

// window.customElements.define('hb-tabb', Tabb);