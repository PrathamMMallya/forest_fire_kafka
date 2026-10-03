import React from 'react'
import DashboardOverview from '../components/DashboardOverview'

function OverviewPage({apiUrl}){
  return (
    <div>
      <DashboardOverview apiUrl={apiUrl} />
    </div>
  )
}
export default OverviewPage
