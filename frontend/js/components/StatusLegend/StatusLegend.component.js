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

class StatusLegend extends HTMLElement {
  connectedCallback() {
    this.render();
  }

  render() {
    this.innerHTML = `
    <section class="cus-tip">
        <span class="cus-a info-icon status-tooltip">
            <i class="large material-icons">info</i>
            <span class="legend-icon status-tooltip legend_align">Status&nbsp;&nbsp;Legend</span>
        </span>
        <div class="legend-hover">
            <div class="legend-status">
                <span class="excellent"></span>Excellent
            </div>
            <div class="legend-status">
                <span class="good"></span>Good
            </div>
            <div class="legend-status">
                <span class="avg"></span> Average
            </div>
            <div class="legend-status">
                <span class="poor"></span>Poor
            </div>
        </div>
    </section>`;
  }

  constructor() {
    super();
  }
}

window.customElements.define("hb-status-legend", StatusLegend);
