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

import {CLASS_NAMES} from "./../../config/constantData.js";
class Label extends HTMLElement {

    get type() {
        return this.getAttribute('type');
    }

    get text() {
        return this.getAttribute('text');
    }

    connectedCallback() {
        this.render();
    }

    render() {
        let { type, text } = this;
        let className = CLASS_NAMES[type] || 'text';
        this.innerHTML = `
            <div class="label ${className}">${text}</div>
        `;  
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-label', Label);