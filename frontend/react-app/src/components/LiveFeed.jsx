import React, {useEffect, useState} from 'react'

function LiveFeed({apiUrl}){
  const [rows,setRows] = useState([])

  async function load(){
    try{
      const r = await fetch(`${apiUrl}/monitoring/current?n=50`)
      if(!r.ok) return
      const j = await r.json()
      setRows(j.reverse())
    }catch(e){}
  }

  useEffect(()=>{ load(); const i = setInterval(load,3000); return ()=>clearInterval(i)},[apiUrl])

  return (
    <div>
      <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
        <div className="small">Showing most recent live predictions</div>
        <div className="small-muted">Count: {rows.length}</div>
      </div>
      <table className="table" style={{marginTop:8}}>
        <thead><tr><th>prob</th><th>pred</th><th>conf</th><th>lat</th><th>lon</th><th className="small">time</th></tr></thead>
        <tbody>
          {rows.map((r,idx)=> (
            <tr key={idx}>
              <td>{(r.probability*100).toFixed(1)}%</td>
              <td>{r.prediction===1? '🔥' : '✅'}</td>
              <td>{(r.probability*100).toFixed(1)}%</td>
              <td>{r.latitude?.toFixed(3)}</td>
              <td>{r.longitude?.toFixed(3)}</td>
              <td className="small">{r.ts ? new Date(r.ts).toLocaleTimeString() : ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
export default LiveFeed
