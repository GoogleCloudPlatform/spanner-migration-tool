class ListTable extends HTMLElement {
    
    get tabName() {
        return this.getAttribute("tabName");
    }

    get tableName() {
        return this.getAttribute("tableName");
    }

    static get observedAttributes() {
        return ["open"];
    }

    attributeChangedCallback(name, oldValue, newValue) {
        this.render();
    }

    connectedCallback() {
        this.render();
    }

    FormattedObj(RenderingObj) {
        let createIndex = RenderingObj.search("CREATE TABLE");
        let createEndIndex = createIndex + 12;
        RenderingObj = RenderingObj.substring(0, createIndex)+RenderingObj.substring(createIndex, createEndIndex)
        .fontcolor("#4285f4")
        .bold() +RenderingObj.substring(createEndIndex);
        console.log(RenderingObj);
        return RenderingObj;
    }

    render() {
        let { tabName , tableName } = this;
        let RenderingObj ;
        if(tabName === "ddl"){
            RenderingObj = JSON.parse(localStorage.getItem('ddlStatementsContent'));
            RenderingObj = RenderingObj[tableName];
            RenderingObj = this.FormattedObj(RenderingObj);
        }else{
            RenderingObj = JSON.parse(localStorage.getItem('summaryReportContent')); 
            RenderingObj = RenderingObj[tableName];
        }
        
        this.innerHTML =`
         ${ tabName=="ddl" ? `<pre> <code>` : `<div>`}
        <div class='mdc-card ${tabName}-content'>
           ${RenderingObj.split('\n').join(`<span class='sql-c'></span>`)}
        </div>
        ${ tabName=="ddl" ? `</code> </pre>` : `</div>`}
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define("hb-list-table", ListTable);
