import React, {useEffect, useState} from 'react'

function RetrainPage({apiUrl}){
  const [drift, setDrift] = useState(null)
  const [model, setModel] = useState(null)
  const [status, setStatus] = useState(null)
  const [busy, setBusy] = useState(false)

  async function load(){
    try{
      const r1 = await fetch(`${apiUrl}/monitoring/drift/status`)
      if(r1.ok) setDrift(await r1.json())
    }catch(e){}
    try{
      const r2 = await fetch(`${apiUrl}/model`)
      if(r2.ok) setModel(await r2.json())
    }catch(e){}
    try{
      const r3 = await fetch(`${apiUrl}/retrain/status`)
      if(r3.ok) setStatus(await r3.json())
    }catch(e){}
  }

  useEffect(()=>{ load() },[apiUrl])

  async function trigger(force=false){
    setBusy(true)
    try{
      await fetch(`${apiUrl}/retrain?force=${force}`,{method:'POST'})
      // refresh status after a moment
      setTimeout(()=> load(),1500)
    }catch(e){}
    setBusy(false)
  }

  const driftScore = drift?.drift_score ?? 0

  return (
    <div>
      <h3>Model Retraining</h3>
      <div style={{border:'1px solid rgba(255,200,0,0.12)',background:'#0b0f14',padding:16,borderRadius:8}}>
        <div style={{fontWeight:700,color:'#fbbf24'}}>⚠️ Resource Warning</div>
        <div className="small" style={{marginTop:6}}>Retraining can take time and consume significant system resources. Ensure adequate compute is available.</div>
      </div>

      <div style={{display:'flex',gap:12,marginTop:12}}>
        <div className="card" style={{flex:1}}>
          <div style={{fontWeight:700}}>{model?.model_name ?? '—'}</div>
          <div className="small-muted">Current Model</div>
        </div>
        <div className="card" style={{flex:1}}>
          <div style={{fontWeight:700,color: driftScore>0.5 ? '#ef4444' : '#10b981'}}>{drift?.dataset_drift ? 'Recommended' : 'Not recommended'}</div>
          <div className="small-muted">Retrain Trigger — Drift score: {driftScore}</div>
        </div>
        <div className="card" style={{flex:1}}>
          <div style={{fontWeight:700}}>{status?.finished_at ? new Date(status.finished_at).toLocaleString() : (status?.started_at ? 'Running' : '—')}</div>
          <div className="small-muted">Last Retrain</div>
        </div>
      </div>

      <div style={{display:'flex',gap:12,marginTop:12}}>
        <div className="card" style={{flex:1,minHeight:140}}>
          <div style={{fontWeight:700}}>Retrain (Drift Based)</div>
          <div className="small-muted" style={{marginTop:6}}>Only retrain if drift is detected. This will check the current drift status and only trigger retraining if the drift score exceeds the threshold.</div>
          <div style={{marginTop:12}}>
            <button className="button" onClick={()=>trigger(false)} disabled={busy || !drift?.dataset_drift}>Retrain (If Drift)</button>
          </div>
        </div>

        <div className="card" style={{flex:1,minHeight:140,border:'1px solid rgba(255,200,0,0.12)'}}>
          <div style={{fontWeight:700,color:'#f59e0b'}}>Force Retrain</div>
          <div className="small-muted" style={{marginTop:6}}>Retrain regardless of drift status. Use this when you've updated training data or want to refresh the model.</div>
          <div style={{marginTop:12}}>
            <button className="button" onClick={()=>trigger(true)} disabled={busy}>Force Retrain</button>
          </div>
        </div>
      </div>

      <div style={{marginTop:12}} className="small-muted">Retraining pipeline: Prefect + MLflow + DVC</div>
    </div>
  )
}
export default RetrainPage
