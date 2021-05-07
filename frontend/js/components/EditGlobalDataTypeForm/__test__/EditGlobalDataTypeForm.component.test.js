import '../EditGlobalDataTypeForm.componenet.js'

test('edit global data type form is rendered',()=>{
    document.body.innerHTML = '<hb-edit-global-datatype-form></hb-edit-global-datatype-form>'
    let component = document.querySelector('hb-edit-global-datatype-form');
    let table = document.getElementById("global-data-type-table");
    expect(component).not.toBe(null);
    expect(table).not.toBe(null);
})

