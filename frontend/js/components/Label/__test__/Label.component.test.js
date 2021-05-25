import '../Label.component.js'

test('label text should display in the dom' ,()=>{
    document.body.innerHTML = '<hb-label type="sessionHeading" text="sagar"></hb-label>'
    let p = document.querySelector('hb-label div');
    expect(p).not.toBe(null)
    expect(p.innerHTML).toBe('sagar')
})

