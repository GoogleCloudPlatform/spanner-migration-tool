import '../Search.component.js'

test('Search component should render' ,()=>{
    let currentTab = "reportTab"
    document.body.innerHTML =` <hb-search tabid="${currentTab}" class="inlineblock" ></hb-search>`
    let component = document.body.querySelector('hb-search');
    expect(component).not.toBe(null)
    expect(component.innerHTML).not.toBe('')
    expect(document.querySelector('form')).not.toBe(null)
    let input = document.getElementById('search-input');
    expect(input.value).toBe('')
    input.value = "actor"
    expect(input.value).toBe('actor')
})