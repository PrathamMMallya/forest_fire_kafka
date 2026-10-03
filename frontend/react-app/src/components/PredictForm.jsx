import React, {useState} from 'react'

const FEATURES = [
  'latitude','longitude','pr','rmax','rmin','sph','srad','tmmn','tmmx','vs','bi','fm100','fm1000','erc','etr','pet','vpd'
]

function PredictForm({apiUrl}){
  const [values,setValues] = useState(Object.fromEntries(FEATURES.map(f=>[f,0])))
  const [result,setResult] = useState(null)
  const [loading,setLoading]=useState(false)

  function update(f,v){ setValues(s=>({...s,[f]:parseFloat(v)})) }

  const sample = ()=>{
    const defaults = {latitude:34.5,longitude:-118.5,pr:0,rmax:2,rmin:0.2,sph:10,srad:200,tmmn:15,tmmx:22,vs:3,bi:1,fm100:50,fm1000:30,erc:10,etr:0.1,pet:1,vpd:1.2}
    setValues({...values,...defaults})
  }

  const predict = async ()=>{
    setLoading(true); setResult(null)
    try{
      const r = await fetch(`${apiUrl}/predict`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({features:values})})
      if(!r.ok){ setResult({error:await r.text()}); setLoading(false); return }
      const j = await r.json()
      setResult(j)
    }catch(e){ setResult({error:e.message}) }
    setLoading(false)
  }

  return (
    <div>
      <h4>Make a prediction</h4>
      <div className="small">Model conf: <strong>{result?.confidence ? (result.confidence*100).toFixed(1)+'%' : '—'}</strong></div>
      <div style={{maxHeight:220,overflow:'auto',marginTop:8}}>
        {FEATURES.map(f=> (
          <div key={f} style={{display:'flex',gap:8,alignItems:'center',marginBottom:6}}>
            <div style={{width:110}} className="small">{f}</div>
            <input style={{flex:1,padding:6,borderRadius:6}} type="number" value={values[f]} onChange={(e)=>update(f,e.target.value)} />
          </div>
        ))}
      </div>

      <div style={{marginTop:8}}>
        <button className="button" onClick={predict} disabled={loading}>{loading? 'Calling...':'Predict'}</button>
        <button className="button" style={{marginLeft:8,background:'#c7f9db'}} onClick={sample}>Fill sample</button>
      </div>

      <div style={{marginTop:12}}>
        {result?.error && <div style={{color:'#ef4444'}}>Error: {result.error}</div>}
        {result && !result.error && (
          <div>
            <div><strong>Prob:</strong> {(result.wildfire_probability*100).toFixed(1)}%</div>
            <div><strong>Pred:</strong> {result.wildfire_prediction === 1 ? 'High risk 🔥' : 'Low risk ✅'}</div>
            <div className="small">Model: {result.model_name}</div>
            <pre style={{background:'rgba(0,0,0,0.2)',padding:8,borderRadius:8}}>{JSON.stringify(result,null,2)}</pre>
          </div>
        )}
      </div>
    </div>
  )
}
export default PredictForm
