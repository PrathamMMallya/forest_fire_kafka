import React from 'react'

function Sidebar({selected='overview', onNavigate=()=>{}}){
  const items = [
    {key:'overview',label:'Overview'},
    {key:'live',label:'Live Predictions'},
    {key:'drift',label:'Drift Monitoring'},
    {key:'model',label:'Model Comparison'},
    {key:'retrain',label:'Retrain Model'},
  ]

  return (
    <aside className="sidebar card">
      <div className="brand">
        <div className="logo">🔥</div>
        <div className="title">Wildfire Watch</div>
      </div>

      <nav className="nav">
        {items.map(it=> (
          <div key={it.key} className={"nav-item" + (selected===it.key? ' active':'')} onClick={()=>onNavigate(it.key)}>
            {it.label}
          </div>
        ))}
      </nav>

      <div className="sidebar-footer small">System Status<br /><span style={{color:'#34d399'}}>● All systems operational</span></div>
    </aside>
  )
}
export default Sidebar
