import React, {useEffect, useState, useMemo} from 'react'
import { Line, Bar, Radar } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, BarElement, RadialLinearScale, Filler, Tooltip, Legend, Title } from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, RadialLinearScale, Filler, Tooltip, Legend, Title)

function SafeNumber(v, fallback=0){
  if(typeof v === 'number' && isFinite(v)) return v
  if(typeof v === 'string'){
    const n = Number(v)
    if(isFinite(n)) return n
  }
  return fallback
}

function formatPercent(v){
  if(v === null || v === undefined) return '—'
  const n = SafeNumber(v, null)
  if(n === null) return '—'
  return `${(n*100).toFixed(1)}%`
}

function SmallMetric({label, value}){
  return <div style={{textAlign:'center'}}>
    <div className="small-muted">{label}</div>
    <div style={{fontWeight:800,fontSize:20,marginTop:6}}>{formatPercent(value)}</div>
  </div>
}

function ModelComparisonPage({apiUrl}){
  const [models, setModels] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)
  const [runDetails, setRunDetails] = useState(null)
  const [runMetrics, setRunMetrics] = useState({})

  async function load(){
    setLoading(true); setError(null)
    try{
      const r = await fetch(`${apiUrl}/models`)
      if(!r.ok){
        let msg = `${r.status} ${r.statusText}`
        try{ const t = await r.text(); if(t) msg += `: ${t}` }catch(_){}
        throw new Error(msg)
      }
      const j = await r.json();
      // Validate response
      if(!Array.isArray(j)){
        setModels([])
        setError(`Unexpected response from server: ${typeof j}`)
        setLoading(false)
        return
      }
      // ensure metrics objects exist
      setModels(j.map(m=>({...m, metrics: m.metrics || {}})))
    }catch(e){ setError(`Unable to fetch models — ${e.message}`); setModels([]) }
    setLoading(false)
  }

  async function loadRun(runId){
    setSelected(runId)
    setRunDetails(null)
    setRunMetrics({})
    try{
      const r = await fetch(`${apiUrl}/models/${runId}`)
      if(r.ok){ const j = await r.json(); setRunDetails(j) }
    }catch(e){ }
    try{
      const r2 = await fetch(`${apiUrl}/models/${runId}/metrics`)
      if(r2.ok){ const j2 = await r2.json(); setRunMetrics(j2) }
    }catch(e){ }
  }

  useEffect(()=>{ load() },[apiUrl])

  // compute best performing model (by accuracy then auc)
  const bestModel = useMemo(()=>{
    if(!models || models.length===0) return null
    let best = null; let bestScore = -Infinity
    models.forEach(m=>{
      const acc = SafeNumber(m.metrics?.accuracy, null)
      const auc = SafeNumber(m.metrics?.auc, null)
      const score = (acc !== null ? acc : (auc !== null ? auc : 0))
      if(score > bestScore){ bestScore = score; best = m }
    })
    return best
  },[models])

  // table rows normalized
  const rows = (models || []).map(m=>({
    name: m.run_name || m.run_id,
    accuracy: SafeNumber(m.metrics?.accuracy, null),
    precision: SafeNumber(m.metrics?.precision, null),
    recall: SafeNumber(m.metrics?.recall, null),
    f1: SafeNumber(m.metrics?.f1, null),
    auc: SafeNumber(m.metrics?.auc, null),
    run_id: m.run_id,
  }))

  // chart data for accuracy comparison
  const accuracyChart = useMemo(()=>{
    const labels = rows.map(r=>r.name)
    const data = rows.map(r=> (r.accuracy !== null ? r.accuracy*100 : 0))
    return { labels, datasets:[{label:'Accuracy', data, backgroundColor:labels.map((l,i)=> rows[i].run_id === bestModel?.run_id ? 'rgba(255,122,24,0.9)' : 'rgba(255,255,255,0.06)') }] }
  },[rows, bestModel])

  const radarData = useMemo(()=>{
    const labels = rows.map(r=>r.name)
    // compute per-model aggregated score for radar as [acc, precision, recall, f1, auc]
    const datasets = rows.map((r,idx)=>({label:r.name, data:[(r.accuracy||0)*100,(r.precision||0)*100,(r.recall||0)*100,(r.f1||0)*100,(r.auc||0)*100], backgroundColor: 'rgba(255,122,24,0.06)', borderColor: idx===0 ? 'rgba(255,122,24,0.9)' : 'rgba(255,255,255,0.12)', fill: true}))
    return { labels: ['Accuracy','Precision','Recall','F1 Score','AUC-ROC'], datasets: datasets.slice(0,4) }
  },[rows])

  return (
    <div>
      <h3>Model Comparison</h3>
      <div className="small">Compare performance metrics across different models</div>

      <div style={{marginTop:12}}>
        {loading && <div className="small">Loading models...</div>}
        {error && (
          <div style={{display:'flex',gap:8,alignItems:'center'}}>
            <div className="card small" style={{color:'#f97316'}}>{error}</div>
            <div><button className="button" onClick={load}>Retry</button></div>
          </div>
        )}

        {/* Top banner: Best performing model */}
        {bestModel && (
          <div className="card-accent" style={{display:'flex',alignItems:'center',gap:20}}>
            <div style={{width:80,height:80,display:'flex',alignItems:'center',justifyContent:'center',background:'linear-gradient(90deg,rgba(255,122,24,0.12),rgba(255,122,24,0.02))',borderRadius:12}}>
              <div style={{fontSize:28,color:'var(--accent)'}}>🏆</div>
            </div>
            <div style={{flex:1}}>
              <div style={{fontSize:14,color:'var(--muted)'}}>Best Performing Model</div>
              <div style={{fontSize:26,fontWeight:800}}>{bestModel.run_name || bestModel.run_id}</div>
              <div style={{color:'var(--accent)',fontWeight:700,marginTop:6}}>{bestModel.metrics?.accuracy ? `${(bestModel.metrics.accuracy*100).toFixed(1)}% Accuracy` : (bestModel.metrics?.auc ? `${(bestModel.metrics.auc*100).toFixed(1)}% AUC` : '')}</div>
            </div>
            <div style={{minWidth:220}}>
              <div className="small-muted">Summary</div>
              <div style={{display:'flex',gap:12,marginTop:8}}>
                <div className="metric">
                  <div className="metric-title">Accuracy</div>
                  <div className="metric-value">{bestModel.metrics?.accuracy ? `${(bestModel.metrics.accuracy*100).toFixed(1)}%` : '—'}</div>
                </div>
                <div className="metric">
                  <div className="metric-title">AUC-ROC</div>
                  <div className="metric-value">{bestModel.metrics?.auc ? `${(bestModel.metrics.auc*100).toFixed(1)}%` : '—'}</div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Performance table */}
        <div className="card" style={{marginTop:12}}>
          <div style={{fontWeight:700,fontSize:18}}>Performance Metrics</div>
          <table className="table" style={{marginTop:12}}>
            <thead>
              <tr>
                <th>Model</th>
                <th>Accuracy</th>
                <th>Precision</th>
                <th>Recall</th>
                <th>F1 Score</th>
                <th>AUC-ROC</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(r=> (
                <tr key={r.run_id} style={r.run_id===bestModel?.run_id ? {background:'linear-gradient(90deg, rgba(255,122,24,0.03), transparent)'}:null}>
                  <td style={{fontWeight:700}}>{r.name}</td>
                  <td>{r.accuracy !== null ? `${(r.accuracy*100).toFixed(1)}%` : '—'}</td>
                  <td>{r.precision !== null ? `${(r.precision*100).toFixed(1)}%` : '—'}</td>
                  <td>{r.recall !== null ? `${(r.recall*100).toFixed(1)}%` : '—'}</td>
                  <td>{r.f1 !== null ? `${(r.f1*100).toFixed(1)}%` : '—'}</td>
                  <td>{r.auc !== null ? `${(r.auc*100).toFixed(1)}%` : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Charts */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:12,marginTop:12}}>
          <div className="card">
            <div style={{fontWeight:700}}>Accuracy Comparison</div>
            <div style={{height:220,marginTop:12}}>
              <Bar data={accuracyChart} options={{plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,max:100}} ,responsive:true,maintainAspectRatio:false}} />
            </div>
          </div>

          <div className="card">
            <div style={{fontWeight:700}}>Multi-metric Comparison</div>
            <div style={{height:220,marginTop:12}}>
              <Radar data={radarData} options={{plugins:{legend:{display:true}},scales:{r:{beginAtZero:true,max:100}} ,responsive:true,maintainAspectRatio:false}} />
            </div>
          </div>
        </div>

      </div>
    </div>
  )
}
export default ModelComparisonPage
