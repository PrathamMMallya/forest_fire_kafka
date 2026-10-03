import React from 'react'
import PredictForm from '../components/PredictForm'
import LiveFeed from '../components/LiveFeed'
import FeatureTrends from '../components/FeatureTrends'

function LivePredictionsPage({apiUrl}){
  return (
    <div className="row">
      <div className="col card" style={{minWidth:320}}>
        <h4>Feature trends</h4>
        <FeatureTrends apiUrl={apiUrl} />
      </div>
      <div className="col card">
        <h4>Live Predictions</h4>
        <LiveFeed apiUrl={apiUrl} />

        <div style={{marginTop:12}} className="card">
          <h4>Make a Prediction</h4>
          <PredictForm apiUrl={apiUrl} />
        </div>
      </div>
    </div>
  )
}
export default LivePredictionsPage
