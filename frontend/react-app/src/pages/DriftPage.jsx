function buildReportUrl(apiUrl){
  const base = apiUrl?.replace(/\/$/, '') ?? '/api'
  // if apiUrl is relative '/api', this will generate '/api/monitoring/drift/report'
  return `${base}/monitoring/drift/report`
}

import React, {useEffect, useState} from 'react'

function DriftPage({apiUrl}){
  const reportUrl = buildReportUrl(apiUrl)
  const [status, setStatus] = useState(null)
  const [err, setErr] = useState(null)

  const [metrics, setMetrics] = useState(null)

  useEffect(()=>{
    let cancelled=false
    async function load(){
      try{
        const r = await fetch(`${apiUrl}/monitoring/drift/status`)
        if(!r.ok){ setErr('No drift report available'); return }
        const j = await r.json()
        if(!cancelled) setStatus(j)
      }catch(e){ setErr('Unable to fetch drift status') }

      try{
        const r2 = await fetch(`${apiUrl}/monitoring/drift/metrics`)
        if(r2.ok){
          const j2 = await r2.json()
          if(!cancelled) setMetrics(j2)
        }
      }catch(e){}
    }
    load()
    return ()=>{ cancelled=true }
  },[apiUrl])

  return (
    <div>
      <h3>Drift Monitoring</h3>
      <div className="small">Interactive drift charts and full report</div>

      <div style={{display:'flex',gap:12,marginTop:12}}>
        <div style={{flex:1}} className="card">
          <h4>Drift dashboard</h4>
          <div className="small">Summary charts and metrics</div>
          <div style={{marginTop:8}}>
            {/* Reuse existing DriftDashboard if desired */}
            <a className="button" style={{marginTop:8}} href={reportUrl} target="_blank" rel="noreferrer">Open full report in new tab</a>
            <div style={{marginTop:8}} className="small-muted">Status: {status? (status.dataset_drift? 'Drift detected' : 'No drift') : (err ?? 'Unknown')}</div>
          </div>
        </div>

        <div style={{flex:1}} className="card">
          <h4>Full drift report</h4>
          <div className="small">Embedded report (may take a moment to load)</div>
          <div style={{marginTop:8}}>
            {err && <div className="small">{err}</div>}
            {!err && <iframe title="drift-report" src={reportUrl} style={{width:'100%',height:600,border:'1px solid rgba(255,255,255,0.04)',borderRadius:8,boxShadow:'0 6px 20px rgba(0,0,0,0.6)'}} />}

            {metrics && (
              <div style={{marginTop:12}}>
                <h4 style={{marginTop:12}}>Top drifted columns</h4>
                <div style={{display:'flex',flexDirection:'column',gap:8,marginTop:8}}>
                  {Object.entries(metrics.result?.drift_by_columns || {}).sort((a,b)=> (b[1].drift_score||0)-(a[1].drift_score||0)).slice(0,6).map(([col,info])=> (
                    <div key={col} style={{display:'flex',alignItems:'center',gap:8}}>
                      <div style={{width:120}} className="small">{col}</div>
                      <div style={{flex:1,height:10,background:'rgba(255,255,255,0.04)',borderRadius:6,overflow:'hidden'}}>
                        <div style={{width:Math.min(100,(info.drift_score||0)),height:10,background:'linear-gradient(90deg,#f97316,#ef4444)'}} />
                      </div>
                      <div style={{width:70,textAlign:'right',fontWeight:700}}>{(info.drift_score||0).toFixed(1)}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

          </div>
        </div>
      </div>
    </div>
  )
}

export default DriftPage
