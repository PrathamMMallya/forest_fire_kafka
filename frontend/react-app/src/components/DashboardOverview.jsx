import React, {useEffect, useState} from 'react'
import { Line } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Title } from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Title)

function smallLine(labels, data){
  return {
    labels,
    datasets:[{label:'Avg prob',data,borderColor:'#ff7a18',backgroundColor:'rgba(255,122,24,0.08)',tension:0.3,pointRadius:2}]
  }
}

function DashboardOverview({apiUrl}){
  const [status,setStatus] = useState(null)
  const [metrics,setMetrics] = useState(null)
  const [rows,setRows] = useState([])
  const [model,setModel] = useState(null)

  async function load(){
    try{
      const [sRes,mRes,rRes,mod] = await Promise.all([
        fetch(`${apiUrl}/monitoring/drift/status`),
        fetch(`${apiUrl}/monitoring/drift/metrics`),
        fetch(`${apiUrl}/monitoring/current?n=200`),
        fetch(`${apiUrl}/model`)
      ])
      if(sRes.ok) setStatus(await sRes.json())
      if(mRes.ok) setMetrics(await mRes.json())
      if(rRes.ok){ const jr = await rRes.json(); setRows(jr.reverse()) }
      if(mod.ok) setModel(await mod.json())
    }catch(e){}
  }

  useEffect(()=>{ load(); const i=setInterval(load,4000); return ()=>clearInterval(i)},[apiUrl])

  const driftScore = status?.drift_score ?? metrics?.score ?? 0
  const datasetDrift = status?.dataset_drift ? true : false
  const retrain = status?.trigger_retrain ? true : false

  // prediction activity, show avg probability recent buckets
  const probs = rows.map(r=> Number(r.probability ?? 0))
  const buckets = 10
  const chunk = Math.max(1, Math.floor(probs.length / buckets))
  const series = []
  for(let i=0;i<probs.length;i+=chunk){
    const chunkarr = probs.slice(i, i+chunk)
    if(chunkarr.length) series.push((chunkarr.reduce((a,b)=>a+b,0)/chunkarr.length))
  }
  const labels = series.map((_,i)=> `T${i+1}`)

  return (
    <div>
      {datasetDrift && (
        <div className="banner card" style={{borderLeft:'4px solid #f59e0b'}}>
          <div>
            <h3>Drift Detected — Retraining Recommended</h3>
            <div className="small">Data drift score ({(driftScore).toFixed(3)}) exceeds threshold. Consider retraining the model.</div>
          </div>
          <div>
            <a className="button" href={`${apiUrl}/monitoring/drift/report`} target="_blank" rel="noreferrer">View Details</a>
          </div>
        </div>
      )}

      <div className="grid metrics" style={{marginTop:12}}>
        <div className="metric card-accent">
          <div className="metric-title">Dataset Drift</div>
          <div className="metric-value">{datasetDrift ? 'YES' : 'NO'}</div>
          <div className="small">Drift detected in data</div>
        </div>
        <div className="metric card-accent">
          <div className="metric-title">Drift Score</div>
          <div className="metric-value">{(driftScore).toFixed(3)}</div>
          <div className="small">Threshold: 0.500</div>
        </div>
        <div className="metric card-accent">
          <div className="metric-title">Retrain Trigger</div>
          <div className="metric-value">{retrain ? 'ON' : 'OFF'}</div>
          <div className="small">Auto-retrain status</div>
        </div>
        <div className="metric card-accent">
          <div className="metric-title">Model</div>
          <div className="metric-value">{model?.model_name ?? '—'}</div>
          <div className="small">Active model in service</div>
        </div>
      </div>

      <div className="row" style={{marginTop:14, gap:12}}>
        <div className="col card" style={{minWidth:420}}>
          <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
            <h4>Prediction Activity</h4>
            <div className="small">Live</div>
          </div>
          <div style={{height:220}}>
            <Line data={smallLine(labels, series)} options={{responsive:true,plugins:{legend:{display:false}},scales:{y:{ticks:{callback:(v)=> (v*100).toFixed(0) + '%'}}}}} />
          </div>
        </div>

        <div className="col card" style={{minWidth:300}}>
          <h4>System Logs</h4>
          <div className="small">Recent activity logs</div>
          <div style={{marginTop:12, maxHeight:200, overflow:'auto'}}>
            {rows.slice(0,8).map((r,idx)=> (
              <div key={idx} style={{padding:'8px 0',borderBottom:'1px solid rgba(255,255,255,0.02)'}}>
                <div style={{display:'flex',justifyContent:'space-between'}}>
                  <div className="small">Model inference completed — prob {(r.probability*100).toFixed(1)}%</div>
                  <div className="small">{r.ts ? new Date(r.ts).toLocaleTimeString() : ''}</div>
                </div>
              </div>
            ))}
            {!rows.length && <div className="small">No logs yet</div>}
          </div>
        </div>
      </div>
    </div>
  )
}
export default DashboardOverview
