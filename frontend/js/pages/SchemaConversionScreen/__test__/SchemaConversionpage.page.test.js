import "./../SchemaConversionScreen.page.js";
import "../../../components/SiteButton/SiteButton.component.js";
import "./../../../components/LoadingSpinner/LoadingSpinner.component.js";

afterEach(()=>{
    while(document.body.firstChild)
    {
        document.body.removeChild(document.body.firstChild)
    }
})

test("empty data test",()=>{
    document.body.innerHTML = '<div><hb-loading-spinner></hb-loading-spinner><hb-schema-conversion-screen></hb-schema-conversion-screen></div>';
    let btn = document.getElementsByTagName('hb-site-button');
    // console.log(btn[0]);
    expect(btn.length).toBe(0);
})

test("button rendering test",()=>{
    document.body.innerHTML = '<div><hb-loading-spinner></hb-loading-spinner><hb-schema-conversion-screen></hb-schema-conversion-screen></div>';
    
})