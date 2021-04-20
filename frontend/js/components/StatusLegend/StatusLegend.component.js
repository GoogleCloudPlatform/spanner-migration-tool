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
