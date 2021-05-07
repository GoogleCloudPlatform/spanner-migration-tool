import "./../Tab.component.js";
import "./../../LoadingSpinner/LoadingSpinner.component.js"
import Store from "./../../../services/Store.service.js"

afterEach(()=>{
    while(document.body.firstChild)
    {
        document.body.removeChild(document.body.firstChild)
    }
})

test('current tab features',()=>{
    document.body.innerHTML = `<hb-tab currentTab=${report} ><hb-tab/>`;
    let tab = document.querySelector('#reportTab')
    expect(tab.className).toBe("nav-link active show")

})

test('disabled tab features',()=>{
    let currenttab = Store.getinstance().currentTab;
    document.body.innerHTML = `<div><hb-loading-spinner></hb-loading-spinner> <hb-tab currentTab=${currenttab}><hb-tab/></div>`;
    let tab = document.querySelector('#ddlTab')
    expect(tab.className).toBe("nav-link ")
    let switchtab = document.getElementsByTagName('hb-tab')[0];
    expect(switchtab.tabId).toBe("ddl")
    switchtab.click();
    console.log("something")
    // expect(tab.className).toBe("nav-link active show")

})