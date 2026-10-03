import React, {useEffect, useState, useCallback} from 'react'
import Sidebar from './components/Sidebar'
import DashboardOverview from './components/DashboardOverview'
import LiveFeed from './components/LiveFeed'
import DriftDashboard from './components/DriftDashboard'
import FeatureTrends from './components/FeatureTrends'

import OverviewPage from './pages/OverviewPage'
import LivePredictionsPage from './pages/LivePredictionsPage'
import DriftPage from './pages/DriftPage'
import ModelComparisonPage from './pages/ModelComparisonPage'
import RetrainPage from './pages/RetrainPage'

function App(){
  const [apiUrl,setApiUrl] = useState(import.meta.env.VITE_API_URL ?? '/api')
  const [health, setHealth] = useState('unknown')
  const [modelInfo, setModelInfo] = useState(null)

  const checkHealth = useCallback(async ()=>{
    try{
      const r = await fetch(`${apiUrl}/`)
      const j = await r.json()
      setHealth(j.status || 'ok')
    }catch(e){
      setHealth('unreachable')
    }
  },[apiUrl])

  useEffect(()=>{checkHealth()},[checkHealth])

  useEffect(()=>{
    let cancelled=false
    async function loadModel(){
      try{
        const r = await fetch(`${apiUrl}/model`)
        if(!r.ok) return
        const j = await r.json()
        if(!cancelled) setModelInfo(j)
      }catch(e){}
    }
    loadModel()
    return ()=>{cancelled=true}
  },[apiUrl])

  const [page, setPage] = useState('overview')

  function renderPage(){
    switch(page){
      case 'overview': return <OverviewPage apiUrl={apiUrl} />
      case 'live': return <LivePredictionsPage apiUrl={apiUrl} />
      case 'drift': return <DriftPage apiUrl={apiUrl} />
      case 'model': return <ModelComparisonPage apiUrl={apiUrl} />
      case 'retrain': return <RetrainPage apiUrl={apiUrl} />
      default: return <OverviewPage apiUrl={apiUrl} />
    }
  }

  return (
    <div className="container">
      <Sidebar selected={page} onNavigate={setPage} />
      <main className="main">
        <div className="header">
          <div>
            <h2>{page === 'overview' ? 'Dashboard Overview' : page === 'live' ? 'Live Predictions' : page === 'drift' ? 'Drift Monitoring' : page === 'model' ? 'Model Comparison' : 'Retrain'}</h2>
            <div className="small">Real-time monitoring of your wildfire prediction system</div>
          </div>

          <div style={{textAlign:'right'}}>
            <div className="small">API URL</div>
            <input value={apiUrl} onChange={(e)=>setApiUrl(e.target.value)} style={{padding:'6px',borderRadius:8,marginRight:8}} />
            <button className="button" onClick={checkHealth}>Check</button>
            <div className="small" style={{marginTop:6}}>Status: <span className="badge">{health}</span></div>
            <div className="small" style={{marginTop:6}}>Model: <span className="badge">{modelInfo?.model_name ?? '—'}</span></div>
          </div>
        </div>

        <div className="card">
          {renderPage()}
        </div>

        <div className="footer">⚠️ Note: If the frontend runs in Docker, use the `/api` proxy (configured in nginx) to reach the inference service.</div>
      </main>
    </div>
  )
}

export default App
