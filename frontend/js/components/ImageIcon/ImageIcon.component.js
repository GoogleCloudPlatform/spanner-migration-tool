import "../Label/Label.component.js";

class ImageIcon extends HTMLElement {
    
  get image() {
    return this.getAttribute("image");
  }
  get label() {
    return this.getAttribute("label");
  }
  get clickAction() {
    return this.getAttribute("clickAction");
  }
  get imageAltText() {
    return this.getAttribute("imageAltText");
  }
  get modalDataTarget() {
    return this.getAttribute("modalDataTarget");
  }

  connectedCallback() {
    this.render();
  }

  render() {
    let { image, label, imageAltText, modalDataTarget } = this;
    this.innerHTML = `
            <div class="image-icon" data-target="${modalDataTarget}" data-toggle="modal" data-backdrop="static" data-keyboard="false">
                <div class="pointer image">
                    <img src="${image}" width="64" height="64" alt="${imageAltText}">
                </div>
                <div class="label pointer">
                    <hb-label type="text" text="${label}" />
                </div>
            </div> 
        `;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-image-icon", ImageIcon);
