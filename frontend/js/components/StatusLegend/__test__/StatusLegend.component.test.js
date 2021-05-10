import '../StatusLegend.component.js'

test('Status legend component render fine' ,()=>{
    document.body.innerHTML = '<hb-status-legend></hb-status-legend>'
    let statuslegend = document.querySelector('hb-status-legend');
    expect(statuslegend).not.toBe(null)
    expect(statuslegend.innerHTML).not.toBe('')
    expect(document.querySelector('.legend-hover')).not.toBe(null)
})

