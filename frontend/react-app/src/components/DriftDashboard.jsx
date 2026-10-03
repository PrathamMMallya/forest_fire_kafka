import React, {useEffect, useState} from 'react'
import { Bar } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend } from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend)

function DriftDashboard({apiUrl}){
  const [status,setStatus] = useState(null)
  const [metrics,setMetrics] = useState(null)

  async function load(){
    try{
      const [sRes,mRes] = await Promise.all([fetch(`${apiUrl}/monitoring/drift/status`), fetch(`${apiUrl}/monitoring/drift/metrics`)])
      if(sRes.ok) setStatus(await sRes.json())
      if(mRes.ok) setMetrics(await mRes.json())
    }catch(e){}
  }

  useEffect(()=>{ load(); const i = setInterval(load,5000); return ()=>clearInterval(i)},[apiUrl])

  const chartData = () => {
    if(!metrics) return {labels:[],datasets:[]}

    // metrics.json is a rich object; try to extract per-column drift scores
    const table = metrics.metrics?.find(m=> m.metric === 'DataDriftTable')?.result
    if(!table) return {labels:[],datasets:[]}

    const byColumns = table.drift_by_columns || {}
    const items = Object.values(byColumns).map(c => ({name: c.column_name, score: c.drift_score || 0}))
    // sort desc and limit top 12
    const top = items.sort((a,b)=>b.score-a.score).slice(0,12)
    const labels = top.map(t=>t.name)
    const data = top.map(t=>t.score)

    return {labels, datasets:[{label:'Drift score',data, backgroundColor:'rgba(6,182,212,0.9)', borderColor:'#06b6d4'}]}
  }

  const chartOptions = {
    responsive:true,
    plugins:{legend:{display:false},tooltip:{callbacks:{label:(ctx)=> `score: ${ctx.parsed.y?.toFixed(3) ?? ctx.formattedValue}`}}},
    scales:{y:{ticks:{beginAtZero:true,precision:3}},x:{ticks:{autoSkip:true,maxRotation:0}}}
  }

  return (
    <div>
      <h4>Drift</h4>
      <div className="small">Drift detected: <strong>{status?.dataset_drift ? 'YES' : 'NO'}</strong></div>
      <div style={{height:180,marginTop:8}}>
        {metrics ? <Bar data={chartData()} options={chartOptions} /> : <div className="small">No metrics available</div>}
      </div>
      <div style={{marginTop:8}}>
        <a className="small" href={`${apiUrl}/monitoring/drift/report`} target="_blank" rel="noreferrer">Open full drift report</a>
      </div>
    </div>
  )
}
export default DriftDashboard
