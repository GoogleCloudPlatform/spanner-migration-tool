import "./../Tab.component.js";
import "./../../LoadingSpinner/LoadingSpinner.component.js"
import Store from "./../../../services/Store.service.js"

describe('enabled tab features',()=> {
    afterEach(() => {
        while (document.body.firstChild) {
            document.body.removeChild(document.body.firstChild)
        }
    })

    test('current tab features', () => {
        document.body.innerHTML = `<hb-tab currentTab="reportTab" ><hb-tab/>`;
        let tab = document.querySelector('#reportTab')
        expect(tab.className).toBe("nav-link active show")
        let otherTab = document.querySelector('#ddlTab')
        expect(otherTab.className).toBe("nav-link ")
    })

    test('total tabs', () => {
        document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab="reportTab"><hb-tab/></div>`;
        let tabsarray = document.querySelectorAll('li.nav-item');
        expect(tabsarray.length).toBe(3);
    })
})

describe('disabled tab features', () => {
    afterEach(() => {
        while (document.body.firstChild) {
            document.body.removeChild(document.body.firstChild)
        }
    })

    let currenttab = Store.getinstance().currentTab;
    document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab=${currenttab}><hb-tab/></div>`;
    let tab = document.querySelector('#ddlTab')
    expect(tab.className).toBe("nav-link ")

    tab.click();
    currenttab = Store.getinstance().currentTab
    document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab=${currenttab}><hb-tab/></div>`;
    tab = document.querySelector('#ddlTab')
    expect(tab.className).toBe("nav-link active show")
})