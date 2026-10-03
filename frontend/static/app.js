// frontend/static/app.js

const FEATURES = [
  'latitude', 'longitude', 'pr', 'rmax', 'rmin', 'sph', 'srad',
  'tmmn', 'tmmx', 'vs', 'bi', 'fm100', 'fm1000', 'erc', 'etr', 'pet', 'vpd'
];

const form = document.getElementById('features-form');
const apiInput = document.getElementById('api-url');
const statusSpan = document.getElementById('backend-status');
const resultEl = document.getElementById('result');

function makeField(name) {
  const label = document.createElement('label');
  label.innerHTML = `<span>${name}</span>`;
  const input = document.createElement('input');
  input.type = 'number';
  input.step = 'any';
  input.name = name;
  input.value = '0';
  label.appendChild(input);
  return label;
}

function buildForm() {
  FEATURES.forEach((f) => {
    form.appendChild(makeField(f));
  });
}

async function checkHealth() {
  const api = apiInput.value;
  try {
    const resp = await fetch(`${api}/`);
    const data = await resp.json();
    statusSpan.textContent = data.status || 'ok';
    statusSpan.style.color = 'green';
  } catch (e) {
    statusSpan.textContent = 'unreachable';
    statusSpan.style.color = 'red';
  }
}

function getFormValues() {
  const formData = new FormData(form);
  const features = {};
  FEATURES.forEach((f) => {
    const value = formData.get(f);
    features[f] = parseFloat(value === null || value === '' ? 0 : value);
  });
  return features;
}

function fillSample() {
  // Some reasonable sample defaults
  const defaults = {
    latitude: 34.5,
    longitude: -118.5,
    pr: 0.0,
    rmax: 2.0,
    rmin: 0.2,
    sph: 10.0,
    srad: 200.0,
    tmmn: 15.0,
    tmmx: 22.0,
    vs: 3.0,
    bi: 1.0,
    fm100: 50.0,
    fm1000: 30.0,
    erc: 10.0,
    etr: 0.1,
    pet: 1.0,
    vpd: 1.2,
  };
  FEATURES.forEach((f) => {
    const el = form.querySelector(`[name=${CSS.escape(f)}]`);
    if (el && defaults[f] !== undefined) el.value = defaults[f];
  });
}

async function predict() {
  const api = apiInput.value;
  const features = getFormValues();
  resultEl.textContent = 'Calling API...';
  try {
    const resp = await fetch(`${api}/predict`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ features }),
    });

    if (!resp.ok) {
      const text = await resp.text();
      resultEl.innerHTML = `<div style="color: #b91c1c;">Error: ${resp.status} — ${text}</div>`;
      return;
    }

    const data = await resp.json();
    const prob = (data.wildfire_probability ?? 0);
    const pred = data.wildfire_prediction;
    const probPct = (prob * 100).toFixed(1);

    resultEl.innerHTML = `
      <div><strong>Probability:</strong> ${probPct}%</div>
      <div><strong>Prediction:</strong> ${pred === 1 ? 'High risk 🔥' : 'Low risk ✅'}</div>
      <pre>${JSON.stringify(data, null, 2)}</pre>
    `;
  } catch (e) {
    resultEl.innerHTML = `<div style="color: #b91c1c;">Request failed: ${e.message}</div>`;
  }
}

// Setup
buildForm();

document.getElementById('check').addEventListener('click', (e) => {
  checkHealth();
});

document.getElementById('fill-sample').addEventListener('click', (e) => {
  fillSample();
});

document.getElementById('predict').addEventListener('click', (e) => {
  predict();
});

// initial health check
checkHealth();
