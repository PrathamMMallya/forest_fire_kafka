import React, {useEffect, useState} from 'react'
import { Line } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Tooltip } from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip)

const FEATURES = [
  'latitude','longitude','pr','rmax','rmin','sph','srad','tmmn','tmmx','vs','bi','fm100','fm1000','erc','etr','pet','vpd'
]

function smallData(series){
  // series is array of rows (most recent first)
  const labels = series.map((_,i)=> i+1).reverse()
  return {labels, datasets: [{data: series, borderColor:'#06b6d4', backgroundColor:'rgba(6,182,212,0.08)', tension:0.3, pointRadius:0}]}
}

function FeatureTrends({apiUrl}){
  const [rows,setRows] = useState([])
  const featuresToPlot = FEATURES.filter(f=> f!=='latitude' && f!=='longitude')

  async function load(){
    try{
      const r = await fetch(`${apiUrl}/monitoring/current?n=200`)
      if(!r.ok) return
      const j = await r.json()
      setRows(j.reverse())
    }catch(e){}
  }

  useEffect(()=>{ load(); const i=setInterval(load,4000); return ()=>clearInterval(i)},[apiUrl])

  if(rows.length===0) return <div className="small">No live data yet</div>

  const maxCharts = FEATURES.length
  const charts = FEATURES.map((f)=>{
    const series = rows.map(r=> Number(r[f] ?? 0)).slice(-30)
    const last = series.length ? series[series.length-1] : 0
    return (
      <div key={f} style={{width:'22%',minWidth:140,margin:'8px 6px',background:'#0b1220',padding:8,borderRadius:8}}>
        <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:6}}>
          <div className="small" style={{fontWeight:600}}>{f}</div>
          <div style={{fontWeight:700,fontSize:16}}>{last !== undefined ? last.toFixed(2) : '—'}</div>
        </div>
        <div style={{height:70}}>
          <Line data={smallData(series)} options={{plugins:{legend:{display:false},tooltip:{callbacks:{label:(ctx)=> ctx.parsed.y?.toFixed(3)}}},responsive:true,maintainAspectRatio:false,scales:{x:{display:false},y:{display:false}}}} />
        </div>
      </div>
    )
  })

  return (
    <div>
      <h4>Feature trends</h4>
      <div className="small">Recent values (sparklines for key features)</div>
      <div style={{display:'flex',flexWrap:'wrap',marginTop:10}}>
        {charts}
      </div>
    </div>
  )
}
export default FeatureTrends
