"""
EWURA LOIS Migration Portal & Lookup UI
Provides a self-contained, responsive, zero-external-dependency frontend
served directly by FastAPI at `/` and `/portal`.
"""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["00 - Web Portal"])

PORTAL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>EWURA — Migration & Data Portal</title>
  <style>
    :root {
      --primary: #0f3d68;
      --primary-dark: #0a2947;
      --primary-light: #1b5b98;
      --accent: #d97706;
      --accent-light: #f59e0b;
      --bg: #f8fafc;
      --surface: #ffffff;
      --surface-alt: #f1f5f9;
      --text: #0f172a;
      --text-muted: #64748b;
      --border: #e2e8f0;
      --border-focus: #3b82f6;
      --success: #10b981;
      --success-bg: #ecfdf5;
      --warning: #f59e0b;
      --warning-bg: #fffbeb;
      --error: #ef4444;
      --error-bg: #fef2f2;
      --info: #3b82f6;
      --info-bg: #eff6ff;
      --radius: 10px;
      --shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
      --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }

    /* Header */
    header {
      background: linear-gradient(135deg, var(--primary-dark) 0%, var(--primary) 100%);
      color: #fff;
      padding: 1rem 2rem;
      box-shadow: var(--shadow);
    }
    .header-inner {
      max-width: 1200px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 1rem;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .brand-logo {
      background: #fff;
      color: var(--primary);
      font-weight: 900;
      font-size: 1.1rem;
      padding: 0.35rem 0.65rem;
      border-radius: 6px;
      letter-spacing: 1px;
    }
    .brand-title {
      font-size: 1.25rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .brand-subtitle {
      font-size: 0.8rem;
      color: #cbd5e1;
    }
    .header-actions {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .env-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.25rem 0.65rem;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .env-prod { background: #065f46; color: #a7f3d0; border: 1px solid #10b981; }
    .env-staging { background: #78350f; color: #fde68a; border: 1px solid #f59e0b; }
    .env-dev { background: #1e3a8a; color: #bfdbfe; border: 1px solid #3b82f6; }
    .doc-link {
      color: #fff;
      text-decoration: none;
      font-size: 0.85rem;
      font-weight: 600;
      background: rgba(255, 255, 255, 0.15);
      padding: 0.4rem 0.85rem;
      border-radius: 6px;
      transition: background 0.2s;
    }
    .doc-link:hover { background: rgba(255, 255, 255, 0.25); }

    /* Nav Tabs */
    .tabs-bar {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0 2rem;
    }
    .tabs-inner {
      max-width: 1200px;
      margin: 0 auto;
      display: flex;
      gap: 0.5rem;
      overflow-x: auto;
    }
    .tab-btn {
      padding: 0.85rem 1.25rem;
      border: none;
      background: none;
      color: var(--text-muted);
      font-weight: 600;
      font-size: 0.95rem;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      border-bottom: 3px solid transparent;
      transition: all 0.2s;
      white-space: nowrap;
    }
    .tab-btn:hover { color: var(--primary); }
    .tab-btn.active {
      color: var(--primary);
      border-bottom-color: var(--primary);
    }

    /* Container */
    main {
      flex: 1;
      max-width: 1200px;
      width: 100%;
      margin: 1.5rem auto;
      padding: 0 1.5rem;
    }

    .tab-content { display: none; }
    .tab-content.active { display: block; }

    /* Cards */
    .card {
      background: var(--surface);
      border-radius: var(--radius);
      border: 1px solid var(--border);
      padding: 1.5rem;
      box-shadow: var(--shadow);
      margin-bottom: 1.5rem;
    }
    .card-header {
      margin-bottom: 1rem;
      padding-bottom: 0.75rem;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.5rem;
    }
    .card-title {
      font-size: 1.15rem;
      font-weight: 700;
      color: var(--primary-dark);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .card-desc {
      font-size: 0.85rem;
      color: var(--text-muted);
      margin-top: 0.25rem;
    }

    /* Steps Guide Banner */
    .guide-banner {
      background: linear-gradient(to right, #eff6ff, #f8fafc);
      border-left: 4px solid var(--info);
      padding: 1rem 1.25rem;
      border-radius: 6px;
      margin-bottom: 1.5rem;
    }
    .guide-banner h4 {
      color: var(--primary-dark);
      font-size: 0.95rem;
      margin-bottom: 0.4rem;
      display: flex;
      align-items: center;
      gap: 0.4rem;
    }
    .guide-banner ol {
      padding-left: 1.4rem;
      font-size: 0.85rem;
      color: #334155;
    }
    .guide-banner li { margin-bottom: 0.25rem; }

    /* Steps Layout */
    .steps-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 1.5rem;
    }
    .step-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
      position: relative;
      box-shadow: var(--shadow);
      transition: transform 0.15s, border-color 0.15s;
    }
    .step-card:hover {
      border-color: var(--primary-light);
      transform: translateY(-2px);
    }
    .step-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      font-size: 0.75rem;
      font-weight: 700;
      padding: 0.2rem 0.5rem;
      border-radius: 4px;
      width: fit-content;
      margin-bottom: 0.75rem;
    }
    .step-1-badge { background: #fee2e2; color: #991b1b; }
    .step-2-badge { background: #fef3c7; color: #92400e; }
    .step-3-badge { background: #e0e7ff; color: #3730a3; }
    .step-header {
      font-size: 1.05rem;
      font-weight: 700;
      color: var(--primary-dark);
      margin-bottom: 0.35rem;
    }
    .step-details {
      font-size: 0.825rem;
      color: var(--text-muted);
      margin-bottom: 1rem;
      flex: 1;
    }

    /* Forms & Inputs */
    .form-group {
      margin-bottom: 1rem;
    }
    .form-label {
      display: block;
      font-size: 0.825rem;
      font-weight: 600;
      color: #334155;
      margin-bottom: 0.35rem;
    }
    .form-select, .form-input {
      width: 100%;
      padding: 0.6rem 0.75rem;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 0.9rem;
      background: #fff;
      color: var(--text);
      transition: border 0.15s, box-shadow 0.15s;
    }
    .form-select:focus, .form-input:focus {
      outline: none;
      border-color: var(--border-focus);
      box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
    }

    /* File Drop Area */
    .file-dropzone {
      border: 2px dashed #cbd5e1;
      border-radius: 8px;
      padding: 1.25rem 1rem;
      text-align: center;
      background: var(--surface-alt);
      cursor: pointer;
      transition: all 0.2s;
      margin-bottom: 1rem;
      position: relative;
    }
    .file-dropzone:hover, .file-dropzone.dragover {
      border-color: var(--primary);
      background: #f0fdf4;
    }
    .file-dropzone input[type="file"] {
      position: absolute;
      top: 0; left: 0; width: 100%; height: 100%;
      opacity: 0;
      cursor: pointer;
    }
    .dropzone-icon {
      font-size: 1.75rem;
      color: var(--primary);
      margin-bottom: 0.25rem;
    }
    .dropzone-text {
      font-size: 0.85rem;
      font-weight: 600;
      color: #334155;
    }
    .dropzone-hint {
      font-size: 0.75rem;
      color: var(--text-muted);
    }
    .file-chosen-name {
      font-size: 0.825rem;
      font-weight: 700;
      color: var(--primary);
      margin-top: 0.4rem;
      word-break: break-all;
    }

    /* Buttons */
    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      padding: 0.65rem 1.25rem;
      border-radius: 6px;
      font-size: 0.9rem;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: all 0.2s;
      width: 100%;
    }
    .btn-primary {
      background: var(--primary);
      color: #fff;
    }
    .btn-primary:hover { background: var(--primary-light); }
    .btn-secondary {
      background: #e2e8f0;
      color: #334155;
    }
    .btn-secondary:hover { background: #cbd5e1; }
    .btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }

    /* Status & Alerts */
    .status-box {
      margin-top: 1rem;
      padding: 0.85rem 1rem;
      border-radius: 6px;
      font-size: 0.85rem;
      display: none;
      word-break: break-word;
    }
    .status-success {
      background: var(--success-bg);
      border: 1px solid var(--success);
      color: #065f46;
    }
    .status-error {
      background: var(--error-bg);
      border: 1px solid var(--error);
      color: #991b1b;
    }
    .status-info {
      background: var(--info-bg);
      border: 1px solid var(--info);
      color: #1e40af;
    }

    /* Lookup Styles */
    .search-bar-wrap {
      display: flex;
      gap: 0.75rem;
      flex-wrap: wrap;
      margin-bottom: 1rem;
    }
    .search-input-wrap {
      flex: 1;
      min-width: 280px;
    }
    .quick-samples {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      flex-wrap: wrap;
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-bottom: 1.5rem;
    }
    .sample-pill {
      background: #e2e8f0;
      color: #334155;
      padding: 0.2rem 0.5rem;
      border-radius: 4px;
      cursor: pointer;
      font-family: monospace;
      font-size: 0.775rem;
      transition: background 0.15s;
    }
    .sample-pill:hover { background: #cbd5e1; color: var(--primary); }

    /* Results Card */
    .result-view {
      display: none;
    }
    .result-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      flex-wrap: wrap;
      gap: 0.75rem;
      margin-bottom: 1rem;
    }
    .result-title {
      font-size: 1.25rem;
      font-weight: 800;
      color: var(--primary-dark);
    }
    .badges-row {
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
    }
    .badge {
      display: inline-block;
      padding: 0.2rem 0.6rem;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 700;
    }
    .badge-approved { background: #d1fae5; color: #065f46; }
    .badge-license { background: #dbeafe; color: #1e40af; }
    .badge-type { background: #fef3c7; color: #92400e; }

    .details-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
      margin-bottom: 1.25rem;
    }
    .detail-item {
      background: var(--surface-alt);
      padding: 0.75rem;
      border-radius: 6px;
    }
    .detail-label {
      font-size: 0.725rem;
      color: var(--text-muted);
      text-transform: uppercase;
      font-weight: 700;
      letter-spacing: 0.05em;
      margin-bottom: 0.2rem;
    }
    .detail-val {
      font-size: 0.95rem;
      font-weight: 600;
      color: var(--text);
      word-break: break-word;
    }
    .highlight-cert {
      border: 1px solid #93c5fd;
      background: #eff6ff;
    }

    /* JSON toggle */
    details summary {
      cursor: pointer;
      font-size: 0.8rem;
      font-weight: 600;
      color: var(--primary);
      margin-top: 0.5rem;
    }
    pre.json-view {
      background: #0f172a;
      color: #38bdf8;
      padding: 1rem;
      border-radius: 6px;
      font-size: 0.775rem;
      overflow-x: auto;
      margin-top: 0.5rem;
    }

    /* Spinner */
    .spinner {
      display: inline-block;
      width: 1rem;
      height: 1rem;
      border: 2px solid rgba(255,255,255,0.3);
      border-radius: 50%;
      border-top-color: #fff;
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    footer {
      background: #fff;
      border-top: 1px solid var(--border);
      padding: 1rem 2rem;
      text-align: center;
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-top: auto;
    }
  </style>
</head>
<body>

  <!-- Header -->
  <header>
    <div class="header-inner">
      <div class="brand">
        <div class="brand-logo">EWURA</div>
        <div>
          <div class="brand-title">LOIS Migration & Operations Portal</div>
          <div class="brand-subtitle">Fast-API Production Service • 10.1.8.144</div>
        </div>
      </div>
      <div class="header-actions">
        <span class="env-badge env-prod" id="headerEnvBadge">Target: PROD (10.1.8.166)</span>
        <a href="/docs" target="_blank" class="doc-link">Swagger API Docs ↗</a>
      </div>
    </div>
  </header>

  <!-- Navigation Tabs -->
  <div class="tabs-bar">
    <div class="tabs-inner">
      <button class="tab-btn active" onclick="switchTab('uploadTab')">
        📁 Step-by-Step File Uploads
      </button>
      <button class="tab-btn" onclick="switchTab('lookupTab')">
        🔍 Lookup by Number
      </button>
      <button class="tab-btn" onclick="switchTab('adminTab')">
        ⚙️ Maintenance & Tools
      </button>
    </div>
  </div>

  <!-- Main Content Area -->
  <main>

    <!-- TAB 1: GUIDED UPLOADS -->
    <div id="uploadTab" class="tab-content active">
      <div class="guide-banner">
        <h4>📋 Recommended Migration Sequence</h4>
        <ol>
          <li><strong>Step 1: Upload Applications First</strong> (e.g. <code>Approved Petroleum License missing.xlsx</code>). Applications must exist in the database before directors or shareholders can be linked.</li>
          <li><strong>Step 2: Upload Shareholders</strong> (e.g. <code>ShareHolder missing.xlsx</code>). Automatically attaches to the applications loaded in Step 1.</li>
          <li><strong>Step 3: Upload Managing Directors</strong> (e.g. <code>Directors Details Missing.xlsx</code>). Automatically attaches to the applications loaded in Step 1.</li>
        </ol>
      </div>

      <div class="steps-grid">

        <!-- STEP 1: APPLICATIONS -->
        <div class="step-card">
          <span class="step-badge step-1-badge">STEP 1 • MANDATORY FIRST</span>
          <div class="step-header">Applications Migration</div>
          <div class="step-details">
            Upload the main license applications file. Creates application records, generates unique certificates, and builds sector details.
          </div>

          <form id="appForm" onsubmit="handleAppUpload(event)">
            <div class="form-group">
              <label class="form-label">Sector Name</label>
              <select class="form-select" id="appSector" required>
                <option value="PETROLEUM" selected>PETROLEUM</option>
                <option value="NATURAL_GAS">NATURAL_GAS</option>
                <option value="ELECTRICITY">ELECTRICITY</option>
                <option value="WATER_SUPPLY">WATER_SUPPLY</option>
              </select>
            </div>

            <div class="form-group">
              <label class="form-label">Target DB Environment</label>
              <select class="form-select" id="appEnv" onchange="syncEnv(this.value)">
                <option value="prod" selected>prod (10.1.8.166:5432)</option>
                <option value="staging">staging</option>
                <option value="dev">dev</option>
              </select>
            </div>

            <div class="file-dropzone" id="appDrop">
              <div class="dropzone-icon">📄</div>
              <div class="dropzone-text">Choose Applications Excel/CSV</div>
              <div class="dropzone-hint">e.g. Approved Petroleum License missing.xlsx</div>
              <div class="file-chosen-name" id="appChosenName"></div>
              <input type="file" id="appFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'appChosenName')" required>
            </div>

            <button type="submit" class="btn btn-primary" id="appSubmitBtn">
              Upload Applications
            </button>
          </form>

          <div class="status-box" id="appStatus"></div>
        </div>

        <!-- STEP 2: SHAREHOLDERS -->
        <div class="step-card">
          <span class="step-badge step-2-badge">STEP 2 • AFTER APPLICATIONS</span>
          <div class="step-header">Shareholders Details</div>
          <div class="step-details">
            Upload company shareholders file. Matches on <code>apprefno</code> to connect each shareholder with its parent application.
          </div>

          <form id="shForm" onsubmit="handleGenericUpload(event, '/api/v1/shareholders/upload', 'shFile', 'shSubmitBtn', 'shStatus', 'Shareholders')">
            <div class="form-group">
              <label class="form-label">Target DB Environment</label>
              <select class="form-select" id="shEnv" onchange="syncEnv(this.value)">
                <option value="prod" selected>prod (10.1.8.166:5432)</option>
                <option value="staging">staging</option>
                <option value="dev">dev</option>
              </select>
            </div>

            <div class="file-dropzone">
              <div class="dropzone-icon">👥</div>
              <div class="dropzone-text">Choose Shareholders Excel/CSV</div>
              <div class="dropzone-hint">e.g. ShareHolder missing.xlsx</div>
              <div class="file-chosen-name" id="shChosenName"></div>
              <input type="file" id="shFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'shChosenName')" required>
            </div>

            <button type="submit" class="btn btn-primary" id="shSubmitBtn">
              Upload Shareholders
            </button>
          </form>

          <div class="status-box" id="shStatus"></div>
        </div>

        <!-- STEP 3: MANAGING DIRECTORS -->
        <div class="step-card">
          <span class="step-badge step-3-badge">STEP 3 • AFTER APPLICATIONS</span>
          <div class="step-header">Managing Directors Details</div>
          <div class="step-details">
            Upload directors and contacts file. Matches on <code>apprefno</code> to attach directors to the uploaded application.
          </div>

          <form id="mdForm" onsubmit="handleGenericUpload(event, '/api/v1/managing-directors/upload', 'mdFile', 'mdSubmitBtn', 'mdStatus', 'Directors')">
            <div class="form-group">
              <label class="form-label">Target DB Environment</label>
              <select class="form-select" id="mdEnv" onchange="syncEnv(this.value)">
                <option value="prod" selected>prod (10.1.8.166:5432)</option>
                <option value="staging">staging</option>
                <option value="dev">dev</option>
              </select>
            </div>

            <div class="file-dropzone">
              <div class="dropzone-icon">👔</div>
              <div class="dropzone-text">Choose Directors Excel/CSV</div>
              <div class="dropzone-hint">e.g. Directors Details Missing.xlsx</div>
              <div class="file-chosen-name" id="mdChosenName"></div>
              <input type="file" id="mdFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'mdChosenName')" required>
            </div>

            <button type="submit" class="btn btn-primary" id="mdSubmitBtn">
              Upload Directors
            </button>
          </form>

          <div class="status-box" id="mdStatus"></div>
        </div>

      </div>
    </div>

    <!-- TAB 2: LOOKUP BY NUMBER -->
    <div id="lookupTab" class="tab-content">
      <div class="card">
        <div class="card-header">
          <div>
            <div class="card-title">🔍 Search Application or License</div>
            <div class="card-desc">Enter either an <strong>Application Number</strong> (e.g. APRL/2025/0469) or a <strong>License/Approval Number</strong> (e.g. PRL-2021-097).</div>
          </div>
        </div>

        <div class="search-bar-wrap">
          <div class="search-input-wrap">
            <input type="text" id="lookupNum" class="form-input" placeholder="e.g. APRL/2025/0469 or PRL-2021-097 or ELD-2023-184" onkeydown="if(event.key==='Enter') executeLookup()">
          </div>
          <div style="width: 140px;">
            <select class="form-select" id="lookupEnv" onchange="syncEnv(this.value)">
              <option value="prod" selected>prod</option>
              <option value="staging">staging</option>
              <option value="dev">dev</option>
            </select>
          </div>
          <div style="width: 150px;">
            <button class="btn btn-primary" onclick="executeLookup()" id="lookupBtn">
              Search Record
            </button>
          </div>
        </div>

        <div class="quick-samples">
          <span>Quick Samples to Test:</span>
          <span class="sample-pill" onclick="setAndSearch('APRL/2025/0469')">APRL/2025/0469</span>
          <span class="sample-pill" onclick="setAndSearch('APRL/2025/0537')">APRL/2025/0537</span>
          <span class="sample-pill" onclick="setAndSearch('PRL-2021-097')">PRL-2021-097</span>
          <span class="sample-pill" onclick="setAndSearch('LPGD-2021-028')">LPGD-2021-028</span>
          <span class="sample-pill" onclick="setAndSearch('ELD-2023-184')">ELD-2023-184</span>
        </div>

        <div class="status-box" id="lookupStatus"></div>

        <!-- Result View -->
        <div class="result-view" id="resultView">
          <div class="card" style="margin-top: 1.5rem; border-color: #cbd5e1;">
            <div class="result-header">
              <div>
                <div class="result-title" id="resFacility">Facility / Applicant Name</div>
                <div style="font-size: 0.85rem; color: var(--text-muted); margin-top: 0.2rem;" id="resNumbersSub">App: - | License: -</div>
              </div>
              <div class="badges-row">
                <span class="badge badge-approved" id="resStatusBadge">APPROVED</span>
                <span class="badge badge-license" id="resLicenseType">LICENSE_PETROLEUM</span>
                <span class="badge badge-type" id="resAppType">RENEW</span>
              </div>
            </div>

            <div class="details-grid">
              <div class="detail-item">
                <div class="detail-label">Application Number</div>
                <div class="detail-val" id="resAppNum">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Approval / License No</div>
                <div class="detail-val" id="resApprovalNo">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Effective Date</div>
                <div class="detail-val" id="resEffDate">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Expire Date</div>
                <div class="detail-val" id="resExpDate">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Reminder Date (Intimate)</div>
                <div class="detail-val" id="resIntDate">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Location</div>
                <div class="detail-val" id="resLocation">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Applicant / Created By</div>
                <div class="detail-val" id="resUser">-</div>
              </div>
              <div class="detail-item highlight-cert">
                <div class="detail-label">Linked Certificate ID</div>
                <div class="detail-val" id="resCertId" style="font-family: monospace; font-size: 0.8rem;">-</div>
              </div>
            </div>

            <details>
              <summary>View Complete JSON Response</summary>
              <pre class="json-view" id="resJsonView"></pre>
            </details>
          </div>
        </div>

      </div>
    </div>

    <!-- TAB 3: ADMIN & UTILITIES -->
    <div id="adminTab" class="tab-content">
      <div class="card">
        <div class="card-header">
          <div class="card-title">⚙️ System Maintenance & Sync Tools</div>
        </div>

        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1.5rem;">
          <div class="step-card">
            <div class="step-header">1. Fix & Sync Certificates</div>
            <div class="step-details">
              Cleans any duplicate certificate rows, syncs certificates with latest approved dates, and re-links all applications to single certificate records.
            </div>
            <button class="btn btn-secondary" onclick="runAdminFix('/api/v1/admin-tools/fix-certificates', 'fixCertStatus')">
              Execute Certificate Sync
            </button>
            <div class="status-box" id="fixCertStatus"></div>
          </div>

          <div class="step-card">
            <div class="step-header">2. Sync License Types</div>
            <div class="step-details">
              Re-evaluates license category rules and updates license_type across applications and certificates.
            </div>
            <button class="btn btn-secondary" onclick="runAdminFix('/api/v1/sync-license-type/preview', 'syncTypeStatus')">
              Preview License Type Changes
            </button>
            <div class="status-box" id="syncTypeStatus"></div>
          </div>
        </div>
      </div>
    </div>

  </main>

  <!-- Footer -->
  <footer>
    EWURA LOIS Migration Engine • Energy and Water Utilities Regulatory Authority
  </footer>

  <script>
    // Tab Switching
    function switchTab(tabId) {
      document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
      
      const targetBtn = Array.from(document.querySelectorAll('.tab-btn')).find(b => b.getAttribute('onclick').includes(tabId));
      if (targetBtn) targetBtn.classList.add('active');
      const targetContent = document.getElementById(tabId);
      if (targetContent) targetContent.classList.add('active');
    }

    // Keep all environment selectors synced
    function syncEnv(val) {
      ['appEnv', 'shEnv', 'mdEnv', 'lookupEnv'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = val;
      });
      const badge = document.getElementById('headerEnvBadge');
      if (badge) {
        badge.className = 'env-badge env-' + val;
        badge.innerText = 'Target: ' + val.toUpperCase() + (val === 'prod' ? ' (10.1.8.166)' : '');
      }
    }

    function showChosenFile(input, targetId) {
      const label = document.getElementById(targetId);
      if (input.files && input.files[0]) {
        label.innerText = '✓ Selected: ' + input.files[0].name + ' (' + (input.files[0].size / 1024).toFixed(1) + ' KB)';
      } else {
        label.innerText = '';
      }
    }

    // Handle Applications Upload
    async function handleAppUpload(e) {
      e.preventDefault();
      const fileInput = document.getElementById('appFile');
      const sector = document.getElementById('appSector').value;
      const dbEnv = document.getElementById('appEnv').value;
      const btn = document.getElementById('appSubmitBtn');
      const statusBox = document.getElementById('appStatus');

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + fileInput.files[0].name + ' to server...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);
      formData.append('sector_name', sector);
      formData.append('db_env', dbEnv);
      formData.append('sync', 'false');

      try {
        const resp = await fetch('/api/v1/application-migrations/upload', {
          method: 'POST',
          body: formData
        });

        const data = await resp.json();
        if (resp.status === 202) {
          statusBox.innerHTML = '<strong>✓ Accepted in Background!</strong> Job ID: <code>' + data.job_id + '</code>.<br>Processing rows in background... Checking status...';
          pollJobStatus(data.job_id, statusBox, btn);
        } else if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ Successfully Processed!</strong><br><pre>' + JSON.stringify(data, null, 2) + '</pre>';
          btn.disabled = false;
          btn.innerText = 'Upload Applications';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Upload failed (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
          btn.disabled = false;
          btn.innerText = 'Upload Applications';
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
        btn.disabled = false;
        btn.innerText = 'Upload Applications';
      }
    }

    // Poll status for async application jobs
    async function pollJobStatus(jobId, statusBox, btn) {
      let attempts = 0;
      const maxAttempts = 60;
      const interval = setInterval(async () => {
        attempts++;
        try {
          const res = await fetch('/api/v1/application-migrations/status/' + jobId);
          if (res.ok) {
            const st = await res.json();
            if (st.status === 'COMPLETED') {
              clearInterval(interval);
              statusBox.className = 'status-box status-success';
              statusBox.innerHTML = '<strong>🎉 Applications Import Completed Successfully!</strong><br>All applications and certificates have been processed and linked.<br><pre>' + JSON.stringify(st.result || st, null, 2) + '</pre>';
              btn.disabled = false;
              btn.innerText = 'Upload Applications';
            } else if (st.status === 'FAILED') {
              clearInterval(interval);
              statusBox.className = 'status-box status-error';
              statusBox.innerHTML = '<strong>❌ Import Failed:</strong> ' + (st.error || 'Unknown error');
              btn.disabled = false;
              btn.innerText = 'Upload Applications';
            } else {
              statusBox.innerHTML = '<strong>Processing in progress...</strong> ' + (st.progress || 'Running...') + ' (Check ' + attempts + ')';
            }
          }
        } catch (e) {
          // ignore transient poll error
        }
        if (attempts >= maxAttempts) {
          clearInterval(interval);
          statusBox.innerHTML += '<br><em>Background job still running. You can check Swagger status later.</em>';
          btn.disabled = false;
          btn.innerText = 'Upload Applications';
        }
      }, 2500);
    }

    // Generic file upload (Shareholders / Directors)
    async function handleGenericUpload(e, url, fileInputId, btnId, statusBoxId, label) {
      e.preventDefault();
      const fileInput = document.getElementById(fileInputId);
      const dbEnv = document.getElementById('appEnv').value;
      const btn = document.getElementById(btnId);
      const statusBox = document.getElementById(statusBoxId);

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + label + ' file to target ' + dbEnv.toUpperCase() + '...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);

      try {
        const fullUrl = url + '?sync=true&background=false&db_env=' + dbEnv;
        const resp = await fetch(fullUrl, {
          method: 'POST',
          body: formData
        });

        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ ' + label + ' Upload Successful!</strong><br><pre>' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = label + ' upload error (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Upload ' + label;
      }
    }

    // Lookup
    function setAndSearch(num) {
      document.getElementById('lookupNum').value = num;
      executeLookup();
    }

    async function executeLookup() {
      const numInput = document.getElementById('lookupNum');
      const val = numInput.value.trim();
      const env = document.getElementById('lookupEnv').value;
      const btn = document.getElementById('lookupBtn');
      const statusBox = document.getElementById('lookupStatus');
      const resultView = document.getElementById('resultView');

      if (!val) {
        numInput.focus();
        return;
      }

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Searching...';
      statusBox.style.display = 'none';
      resultView.style.display = 'none';

      try {
        const url = '/api/v1/admin-tools/lookup-by-number?number=' + encodeURIComponent(val) + '&limit=5&db_env=' + env;
        const resp = await fetch(url);
        const data = await resp.json();

        if (resp.ok && data.results && data.results.length > 0) {
          const rec = data.results[0];
          document.getElementById('resFacility').innerText = rec.resolved_name || rec.facility_name || rec.applicant_name || 'Record Found';
          document.getElementById('resNumbersSub').innerText = 'Application: ' + (rec.application_number || '-') + ' | License: ' + (rec.approval_no || '-');
          document.getElementById('resStatusBadge').innerText = rec.status || 'APPROVED';
          document.getElementById('resLicenseType').innerText = rec.license_type || '-';
          document.getElementById('resAppType').innerText = rec.application_type || 'NEW';

          document.getElementById('resAppNum').innerText = rec.application_number || '-';
          document.getElementById('resApprovalNo').innerText = rec.approval_no || '-';
          document.getElementById('resEffDate').innerText = rec.effective_date ? rec.effective_date.substring(0, 10) : '-';
          document.getElementById('resExpDate').innerText = rec.expire_date ? rec.expire_date.substring(0, 10) : '-';
          document.getElementById('resIntDate').innerText = rec.intimate_date ? rec.intimate_date.substring(0, 10) : '-';
          document.getElementById('resLocation').innerText = [rec.district, rec.region].filter(Boolean).join(', ') || '-';
          document.getElementById('resUser').innerText = rec.username || '-';
          document.getElementById('resCertId').innerText = rec.certificate_id || 'Not linked';

          document.getElementById('resJsonView').innerText = JSON.stringify(data, null, 2);
          resultView.style.display = 'block';
        } else {
          statusBox.style.display = 'block';
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'No application found for "' + val + '" in ' + env.toUpperCase() + '.';
        }
      } catch (err) {
        statusBox.style.display = 'block';
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network request failed: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Search Record';
      }
    }

    // Admin Tools
    async function runAdminFix(url, statusId) {
      const box = document.getElementById(statusId);
      box.style.display = 'block';
      box.className = 'status-box status-info';
      box.innerText = 'Executing operation...';

      try {
        const resp = await fetch(url, { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
          box.className = 'status-box status-success';
          box.innerHTML = '<strong>✓ Done!</strong><br><pre>' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          box.className = 'status-box status-error';
          box.innerText = 'Error: ' + JSON.stringify(data);
        }
      } catch (err) {
        box.className = 'status-box status-error';
        box.innerText = 'Failed: ' + err.message;
      }
    }
  </script>
</body>
</html>
"""

@router.get("/", response_class=HTMLResponse, include_in_schema=False)
@router.get("/portal", response_class=HTMLResponse, include_in_schema=False)
def get_migration_portal():
    """Serves the interactive EWURA Migration Portal Web Interface."""
    return HTMLResponse(content=PORTAL_HTML, status_code=200)
