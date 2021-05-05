import SiteButton from '../SiteButton.component.js'
afterEach(()=>{
    while(document.body.firstChild)
    {
        document.body.removeChild(document.body.firstChild)
    }
})
test('button component rendering and event listener' ,()=>{
    document.body.innerHTML = `<hb-site-button buttonid="test-id" classname="test-class" 
    buttonaction="test-action" text="test Button"></hb-site-button>`
    let p = document.querySelector('hb-site-button');
    let mockFn = jest.fn(()=>p.add(5,6))

    expect(p.innerHTML).not.toBe(null)
    expect(p.text).toBe('test Button')
    expect(p.buttonAction).toBe('test-action')
    expect(p.className).toBe('test-class')
    expect(p.buttonId).toBe('test-id')
    p.addEventListener('click',mockFn)
    p.click()
    expect(mockFn.mock.calls.results[0].value).toBe(11)
})

test('add button',()=>{
    document.body.innerHTML = `<hb-site-button buttonid="test-id" classname="test-class" 
    buttonaction="test-action" text="test Button"></hb-site-button>`
    let p = document.querySelector('hb-site-button');
    expect(p.add(2,2)).toBe(4)
})

