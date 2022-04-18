// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
