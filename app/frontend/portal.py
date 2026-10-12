"""
EWURA LOIS Migration Portal & Operations Center
Provides a complete, responsive, zero-external-dependency frontend
served directly by FastAPI at `/` and `/portal`.
Consumes all data upload pipelines, maintenance tools, transfer operations,
and system diagnostics with step-by-step guidance for first-time users.
"""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["00 - Web Portal"])

PORTAL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>EWURA — LOIS Migration & Operations Portal</title>
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
      max-width: 1400px;
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
      gap: 0.85rem;
    }
    .brand-logo {
      background: #fff;
      color: var(--primary);
      font-weight: 900;
      font-size: 1.15rem;
      padding: 0.35rem 0.75rem;
      border-radius: 6px;
      letter-spacing: 1px;
    }
    .brand-title {
      font-size: 1.25rem;
      font-weight: 800;
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
      flex-wrap: wrap;
    }
    .env-picker-wrap {
      display: flex;
      align-items: center;
      background: rgba(255, 255, 255, 0.12);
      border-radius: 8px;
      padding: 0.25rem 0.6rem;
      gap: 0.5rem;
    }
    .env-picker-label {
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #e2e8f0;
    }
    .env-picker-select {
      background: transparent;
      border: none;
      color: #fff;
      font-weight: 700;
      font-size: 0.85rem;
      outline: none;
      cursor: pointer;
    }
    .env-picker-select option {
      color: #000;
      background: #fff;
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

    /* Navigation Tabs */
    .tabs-bar {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0 2rem;
      position: sticky;
      top: 0;
      z-index: 50;
      box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .tabs-inner {
      max-width: 1400px;
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
      font-weight: 700;
    }

    /* Container */
    main {
      flex: 1;
      max-width: 1400px;
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
      font-size: 1.2rem;
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

    /* Guides & Banners */
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
    .guide-banner ol, .guide-banner ul {
      padding-left: 1.4rem;
      font-size: 0.85rem;
      color: #334155;
    }
    .guide-banner li { margin-bottom: 0.3rem; }

    .first-timer-tip {
      background: #fffbeb;
      border-left: 4px solid var(--warning);
      padding: 0.85rem 1.2rem;
      border-radius: 6px;
      font-size: 0.85rem;
      color: #92400e;
      margin-bottom: 1.25rem;
      display: flex;
      align-items: flex-start;
      gap: 0.6rem;
    }

    /* Section Subheaders */
    .section-headline {
      font-size: 1.1rem;
      font-weight: 700;
      color: var(--primary);
      margin: 1.75rem 0 0.85rem 0;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      padding-bottom: 0.35rem;
      border-bottom: 2px solid #e2e8f0;
    }

    /* Steps Grid */
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
      text-transform: uppercase;
    }
    .badge-primary-order { background: #fee2e2; color: #991b1b; }
    .badge-secondary-order { background: #fef3c7; color: #92400e; }
    .badge-tertiary-order { background: #e0e7ff; color: #3730a3; }
    .badge-tool { background: #f1f5f9; color: #475569; }

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
      margin-bottom: 0.85rem;
    }
    .form-label {
      display: block;
      font-size: 0.8rem;
      font-weight: 600;
      color: #334155;
      margin-bottom: 0.3rem;
    }
    .form-select, .form-input {
      width: 100%;
      padding: 0.55rem 0.75rem;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 0.875rem;
      background: #fff;
      color: var(--text);
      transition: border 0.15s, box-shadow 0.15s;
    }
    .form-select:focus, .form-input:focus {
      outline: none;
      border-color: var(--border-focus);
      box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
    }

    .form-checkbox-label {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      font-size: 0.825rem;
      color: #334155;
      cursor: pointer;
      margin-top: 0.25rem;
      margin-bottom: 0.75rem;
      font-weight: 600;
    }
    .form-checkbox-label input[type="checkbox"] {
      width: 1rem;
      height: 1rem;
      accent-color: var(--primary);
      cursor: pointer;
    }

    /* File Drop Area */
    .file-dropzone {
      border: 2px dashed #cbd5e1;
      border-radius: 8px;
      padding: 1.1rem 1rem;
      text-align: center;
      background: var(--surface-alt);
      cursor: pointer;
      transition: all 0.2s;
      margin-bottom: 0.85rem;
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
      font-size: 1.6rem;
      color: var(--primary);
      margin-bottom: 0.2rem;
    }
    .dropzone-text {
      font-size: 0.85rem;
      font-weight: 600;
      color: #334155;
    }
    .dropzone-hint {
      font-size: 0.725rem;
      color: var(--text-muted);
    }
    .file-chosen-name {
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--primary);
      margin-top: 0.35rem;
      word-break: break-all;
    }

    /* Buttons */
    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      padding: 0.6rem 1.15rem;
      border-radius: 6px;
      font-size: 0.875rem;
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
    .btn-warning {
      background: #f59e0b;
      color: #fff;
    }
    .btn-warning:hover { background: #d97706; }
    .btn-danger {
      background: #ef4444;
      color: #fff;
    }
    .btn-danger:hover { background: #dc2626; }
    .btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }

    /* Status & Alerts */
    .status-box {
      margin-top: 0.85rem;
      padding: 0.85rem 1rem;
      border-radius: 6px;
      font-size: 0.825rem;
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

    /* Interactive Tables */
    .table-responsive {
      max-height: 420px;
      overflow-y: auto;
      border: 1px solid var(--border);
      border-radius: 6px;
      margin-top: 1rem;
    }
    .data-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.825rem;
      text-align: left;
    }
    .data-table th {
      background: var(--surface-alt);
      color: var(--primary-dark);
      font-weight: 700;
      padding: 0.65rem 0.85rem;
      position: sticky;
      top: 0;
      border-bottom: 2px solid var(--border);
      z-index: 10;
    }
    .data-table td {
      padding: 0.6rem 0.85rem;
      border-bottom: 1px solid var(--border);
    }
    .data-table tr:hover td {
      background: #f8fafc;
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
      max-height: 280px;
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
          <div class="brand-subtitle">Energy and Water Utilities Regulatory Authority • Enterprise Operations API</div>
        </div>
      </div>
      <div class="header-actions">
        <div class="env-picker-wrap">
          <span class="env-picker-label">Active Target:</span>
          <select id="globalEnv" class="env-picker-select" onchange="syncEnv(this.value)">
            <option value="prod" selected>PROD (10.1.8.166:5432)</option>
            <option value="staging">STAGING (10.1.8.157:5432)</option>
            <option value="dev">DEV (Local Test)</option>
          </select>
        </div>
        <span class="env-badge env-prod" id="headerEnvBadge">PROD Active</span>
        <a href="/docs" target="_blank" class="doc-link">Swagger Docs ↗</a>
      </div>
    </div>
  </header>

  <!-- Navigation Tabs -->
  <div class="tabs-bar">
    <div class="tabs-inner">
      <button class="tab-btn active" onclick="switchTab('uploadTab')">
        📁 Data Migration Uploads
      </button>
      <button class="tab-btn" onclick="switchTab('lookupTab')">
        🔍 Application & License Inspector
      </button>
      <button class="tab-btn" onclick="switchTab('transferTab')">
        🔄 Ownership Transfer & Recovery
      </button>
      <button class="tab-btn" onclick="switchTab('maintenanceTab')">
        🛠️ Schema, Integrity & Backfills
      </button>
      <button class="tab-btn" onclick="switchTab('referenceTab')">
        📚 Reference Data & Diagnostics
      </button>
    </div>
  </div>

  <!-- Main Content Area -->
  <main>

    <!-- ========================================== -->
    <!-- TAB 1: GUIDED DATA MIGRATION UPLOADS       -->
    <!-- ========================================== -->
    <div id="uploadTab" class="tab-content active">

      <div class="first-timer-tip">
        <div>💡</div>
        <div>
          <strong>First-time Migration Guidance:</strong> Always follow the numbered sequence!
          Parent applications must exist in the database <em>before</em> attaching Shareholders, Directors, or Supervisor details.
          Records match on <code>apprefno</code> (Application Number).
        </div>
      </div>

      <!-- SECTION A: PRIMARY SECTOR APPLICATIONS -->
      <div class="section-headline">
        <span>1. Main License Applications (Mandatory First)</span>
      </div>

      <div class="steps-grid">

        <!-- 1.1 Sector Applications Upload -->
        <div class="step-card">
          <span class="step-badge badge-primary-order">PIPELINE 1 • PRIMARY</span>
          <div class="step-header">Sector Applications (Bulk Migration)</div>
          <div class="step-details">
            Imports Petroleum, Natural Gas, Electricity, or Water Supply applications. Creates applications, assigns categories, and generates linked certificates.
          </div>

          <form id="appForm" onsubmit="handleSectorAppUpload(event)">
            <div class="form-group">
              <label class="form-label">Sector Name</label>
              <select class="form-select" id="appSector" required>
                <option value="PETROLEUM" selected>PETROLEUM</option>
                <option value="NATURAL_GAS">NATURAL_GAS</option>
                <option value="ELECTRICITY">ELECTRICITY</option>
                <option value="WATER_SUPPLY">WATER_SUPPLY</option>
              </select>
            </div>

            <div class="file-dropzone">
              <div class="dropzone-icon">📄</div>
              <div class="dropzone-text">Choose Applications Excel/CSV</div>
              <div class="dropzone-hint">e.g. Approved Petroleum License missing.xlsx</div>
              <div class="file-chosen-name" id="appChosenName"></div>
              <input type="file" id="appFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'appChosenName')" required>
            </div>

            <button type="submit" class="btn btn-primary" id="appSubmitBtn">
              Upload Applications File
            </button>
          </form>

          <div class="status-box" id="appStatus"></div>
        </div>

        <!-- 1.2 Water Supply Specialized Upload -->
        <div class="step-card">
          <span class="step-badge badge-primary-order">PIPELINE 1B • WATER SPECIAL</span>
          <div class="step-header">Water Supply Applications Pipeline</div>
          <div class="step-details">
            Specialized ingestion for water authority applications, populating <code>financial_information</code>, <code>bank_details_tanzania</code>, and government notice attachments.
          </div>

          <form id="waterForm" onsubmit="handleWaterSupplyUpload(event)">
            <div class="file-dropzone">
              <div class="dropzone-icon">💧</div>
              <div class="dropzone-text">Choose Water Supply Excel/CSV</div>
              <div class="dropzone-hint">e.g. Water_Supply_Final.xlsx</div>
              <div class="file-chosen-name" id="waterChosenName"></div>
              <input type="file" id="waterFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'waterChosenName')" required>
            </div>

            <button type="submit" class="btn btn-primary" id="waterSubmitBtn">
              Upload Water Applications
            </button>
          </form>

          <div class="status-box" id="waterStatus"></div>
        </div>

      </div>

      <!-- SECTION B: ELECTRICAL INSTALLATIONS PIPELINE -->
      <div class="section-headline">
        <span>2. Electrical Installations 4-Step Pipeline</span>
      </div>

      <div class="steps-grid">

        <!-- 2.1 Electrical Applications -->
        <div class="step-card">
          <span class="step-badge badge-primary-order">STEP 1 • ELECTRICAL APPS</span>
          <div class="step-header">Electrical Installation Applications</div>
          <div class="step-details">
            Imports applications into <code>application_electrical_installation</code>. Matches on <code>apprefno</code> and <code>installationname</code>.
          </div>
          <form onsubmit="handleElectricalPipelineUpload(event, '/api/v1/electrical-installations/upload', 'elecAppFile', 'elecAppBtn', 'elecAppStatus', 'Electrical Applications')">
            <div class="file-dropzone">
              <div class="dropzone-icon">⚡</div>
              <div class="dropzone-text">Electrical Applications Excel</div>
              <div class="file-chosen-name" id="elecAppChosen"></div>
              <input type="file" id="elecAppFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'elecAppChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="elecAppBtn">Upload Electrical Apps</button>
          </form>
          <div class="status-box" id="elecAppStatus"></div>
        </div>

        <!-- 2.2 Employed Supervisors Experience -->
        <div class="step-card">
          <span class="step-badge badge-secondary-order">STEP 2 • EMPLOYED EXPERIENCE</span>
          <div class="step-header">Employed Experience / Supervisors</div>
          <div class="step-details">
            Attaches supervisor work experience into <code>supervisor_details</code> and <code>employed_experience</code> for electrical contractors.
          </div>
          <form onsubmit="handleElectricalPipelineUpload(event, '/api/v1/electrical-installations/upload-employed-experience', 'elecExpFile', 'elecExpBtn', 'elecExpStatus', 'Employed Experience')">
            <div class="file-dropzone">
              <div class="dropzone-icon">👷</div>
              <div class="dropzone-text">Employed Experience Excel</div>
              <div class="file-chosen-name" id="elecExpChosen"></div>
              <input type="file" id="elecExpFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'elecExpChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="elecExpBtn">Upload Employed Experience</button>
          </form>
          <div class="status-box" id="elecExpStatus"></div>
        </div>

        <!-- 2.3 Certificate Verifications -->
        <div class="step-card">
          <span class="step-badge badge-secondary-order">STEP 3 • CERT VERIFICATIONS</span>
          <div class="step-header">Certificate Verifications</div>
          <div class="step-details">
            Imports supervisor qualification verifications into <code>certificate_verifications</code>.
          </div>
          <form onsubmit="handleElectricalPipelineUpload(event, '/api/v1/electrical-installations/upload-certificate-verifications', 'elecVerFile', 'elecVerBtn', 'elecVerStatus', 'Cert Verifications')">
            <div class="file-dropzone">
              <div class="dropzone-icon">📜</div>
              <div class="dropzone-text">Certificate Verifications Excel</div>
              <div class="file-chosen-name" id="elecVerChosen"></div>
              <input type="file" id="elecVerFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'elecVerChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="elecVerBtn">Upload Verifications</button>
          </form>
          <div class="status-box" id="elecVerStatus"></div>
        </div>

        <!-- 2.4 Self-Employed Projects -->
        <div class="step-card">
          <span class="step-badge badge-secondary-order">STEP 4 • SELF-EMPLOYED</span>
          <div class="step-header">Self-Employed Experience</div>
          <div class="step-details">
            Imports project records into <code>self_employed</code> and <code>custom_details</code> for self-employed licensed wiremen.
          </div>
          <form onsubmit="handleElectricalPipelineUpload(event, '/api/v1/electrical-installations/upload-self-employed', 'elecSelfFile', 'elecSelfBtn', 'elecSelfStatus', 'Self-Employed Experience')">
            <div class="file-dropzone">
              <div class="dropzone-icon">🔨</div>
              <div class="dropzone-text">Self-Employed Excel</div>
              <div class="file-chosen-name" id="elecSelfChosen"></div>
              <input type="file" id="elecSelfFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'elecSelfChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="elecSelfBtn">Upload Self-Employed</button>
          </form>
          <div class="status-box" id="elecSelfStatus"></div>
        </div>

      </div>

      <!-- SECTION C: COMPANY ENTITIES -->
      <div class="section-headline">
        <span>3. Company Ownership & Entity Details (After Applications)</span>
      </div>

      <div class="steps-grid">

        <!-- 3.1 Shareholders -->
        <div class="step-card">
          <span class="step-badge badge-secondary-order">STEP 2 • SHAREHOLDERS</span>
          <div class="step-header">Shareholders Details</div>
          <div class="step-details">
            Upload company shareholders file. Matches on <code>apprefno</code> to link each shareholder to its parent application sector detail.
          </div>
          <form onsubmit="handleGenericFormUpload(event, '/api/v1/shareholders/upload', 'shFile', 'shSubmitBtn', 'shStatus', 'Shareholders')">
            <div class="file-dropzone">
              <div class="dropzone-icon">👥</div>
              <div class="dropzone-text">Choose Shareholders Excel/CSV</div>
              <div class="dropzone-hint">e.g. ShareHolder missing.xlsx</div>
              <div class="file-chosen-name" id="shChosenName"></div>
              <input type="file" id="shFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'shChosenName')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="shSubmitBtn">Upload Shareholders</button>
          </form>
          <div class="status-box" id="shStatus"></div>
        </div>

        <!-- 3.2 Managing Directors -->
        <div class="step-card">
          <span class="step-badge badge-tertiary-order">STEP 3 • DIRECTORS</span>
          <div class="step-header">Managing Directors & Contacts</div>
          <div class="step-details">
            Upload directors and contacts file. Matches on <code>apprefno</code> to attach directors to the existing application.
          </div>
          <form onsubmit="handleGenericFormUpload(event, '/api/v1/managing-directors/upload', 'mdFile', 'mdSubmitBtn', 'mdStatus', 'Directors')">
            <div class="file-dropzone">
              <div class="dropzone-icon">👔</div>
              <div class="dropzone-text">Choose Directors Excel/CSV</div>
              <div class="dropzone-hint">e.g. Directors Details Missing.xlsx</div>
              <div class="file-chosen-name" id="mdChosenName"></div>
              <input type="file" id="mdFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'mdChosenName')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="mdSubmitBtn">Upload Directors</button>
          </form>
          <div class="status-box" id="mdStatus"></div>
        </div>

      </div>

      <!-- SECTION D: SYSTEM FOUNDATION & REFERENCE UPLOADS -->
      <div class="section-headline">
        <span>4. System Foundation & Reference Data Uploads</span>
      </div>

      <div class="steps-grid">

        <!-- 4.1 LOIS Portal Users Upload -->
        <div class="step-card">
          <span class="step-badge badge-tool">SYSTEM USERS</span>
          <div class="step-header">LOIS Portal Users Upload</div>
          <div class="step-details">
            Upload historical user credentials from legacy LOIS database into <code>public.users</code> with password hashes and roles.
          </div>
          <form onsubmit="handleDirectFileUpload(event, '/api/v1/lois-users/upload', 'loisUserFile', 'loisUserBtn', 'loisUserStatus', 'LOIS Users')">
            <div class="file-dropzone">
              <div class="dropzone-icon">👤</div>
              <div class="dropzone-text">Choose LOIS Users Excel</div>
              <div class="dropzone-hint">Columns: username, full_name, email, phone, role</div>
              <div class="file-chosen-name" id="loisUserChosen"></div>
              <input type="file" id="loisUserFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'loisUserChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="loisUserBtn">Upload LOIS Users</button>
          </form>
          <div class="status-box" id="loisUserStatus"></div>
        </div>

        <!-- 4.2 System Categories Upload -->
        <div class="step-card">
          <span class="step-badge badge-tool">CATEGORIES</span>
          <div class="step-header">Categories Upload</div>
          <div class="step-details">
            Import new categories into <code>public.categories</code>. Requires Excel columns: <code>category_name</code> and <code>sector_name</code>.
          </div>
          <form onsubmit="handleDirectFileUpload(event, '/api/v1/categories/upload', 'catFile', 'catBtn', 'catStatus', 'Categories')">
            <div class="file-dropzone">
              <div class="dropzone-icon">🏷️</div>
              <div class="dropzone-text">Choose Categories Excel</div>
              <div class="dropzone-hint">Columns: category_name, sector_name</div>
              <div class="file-chosen-name" id="catChosen"></div>
              <input type="file" id="catFile" accept=".xlsx,.xls" onchange="showChosenFile(this, 'catChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="catBtn">Upload Categories</button>
          </form>
          <div class="status-box" id="catStatus"></div>
        </div>

        <!-- 4.3 License Category Fees -->
        <div class="step-card">
          <span class="step-badge badge-tool">FEES & TARIFFS</span>
          <div class="step-header">License Category Fees Upload</div>
          <div class="step-details">
            Imports license categories and tariff fee schedules into <code>ca_license_categories</code> for the selected sector.
          </div>
          <form onsubmit="handleLicenseCategoryFeesUpload(event)">
            <div class="form-group">
              <label class="form-label">Sector Name</label>
              <select class="form-select" id="licCatSector" required>
                <option value="Petroleum" selected>Petroleum</option>
                <option value="Natural Gas">Natural Gas</option>
                <option value="Electricity">Electricity</option>
                <option value="Water & Wastewater">Water & Wastewater</option>
              </select>
            </div>
            <div class="file-dropzone">
              <div class="dropzone-icon">💰</div>
              <div class="dropzone-text">Choose License Categories & Fees Excel</div>
              <div class="file-chosen-name" id="licCatChosen"></div>
              <input type="file" id="licCatFile" accept=".xlsx,.xls,.csv" onchange="showChosenFile(this, 'licCatChosen')" required>
            </div>
            <button type="submit" class="btn btn-primary" id="licCatBtn">Upload License Category Fees</button>
          </form>
          <div class="status-box" id="licCatStatus"></div>
        </div>

      </div>

    </div>

    <!-- ========================================== -->
    <!-- TAB 2: APPLICATION & LICENSE INSPECTOR     -->
    <!-- ========================================== -->
    <div id="lookupTab" class="tab-content">
      <div class="card">
        <div class="card-header">
          <div>
            <div class="card-title">🔍 Search Application or License</div>
            <div class="card-desc">Enter either an <strong>Application Number</strong> (e.g. APRL/2025/0469) or a <strong>License/Approval Number</strong> (e.g. PRL-2021-097). Automatically resolves applicant name, dates, status, certificate link, and geography.</div>
          </div>
        </div>

        <div class="search-bar-wrap">
          <div class="search-input-wrap">
            <input type="text" id="lookupNum" class="form-input" placeholder="e.g. APRL/2025/0469 or PRL-2021-097 or ELD-2023-184" onkeydown="if(event.key==='Enter') executeLookup()">
          </div>
          <div style="width: 150px;">
            <button class="btn btn-primary" onclick="executeLookup()" id="lookupBtn">
              Search Record
            </button>
          </div>
        </div>

        <div class="quick-samples">
          <span>Quick Samples:</span>
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
                <div class="detail-label">Location (District, Region)</div>
                <div class="detail-val" id="resLocation">-</div>
              </div>
              <div class="detail-item">
                <div class="detail-label">Created By Username</div>
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

    <!-- ========================================== -->
    <!-- TAB 3: OWNERSHIP TRANSFER & RECOVERY       -->
    <!-- ========================================== -->
    <div id="transferTab" class="tab-content">

      <div class="guide-banner">
        <h4>🔄 Application Ownership Governance</h4>
        <p style="font-size: 0.85rem; color: #334155;">
          When an applicant or operator transfers their license or needs account reassignment in LOIS, all associated records
          (application, certificates, and child tables) must be synchronously updated.
          <strong>Always use Step 1 (Preview) first</strong> to verify row counts before applying changes.
          Full automatic backup snapshots are stored in <code>transfer_ownership_backup</code> and can be restored at any time.
        </p>
      </div>

      <div class="steps-grid">

        <!-- 3.1 Full Application Ownership Transfer -->
        <div class="step-card">
          <span class="step-badge badge-primary-order">CORE OPERATION</span>
          <div class="step-header">Full Application Transfer (With Auto-Backup)</div>
          <div class="step-details">
            Transfers <code>applications.created_by</code>, <code>username</code>, <code>certificates.owner_id</code>, and all child tables to a new user account.
          </div>

          <form onsubmit="handleTransferSubmit(event)">
            <div class="form-group">
              <label class="form-label">Application Number</label>
              <input type="text" id="transAppNo" class="form-input" placeholder="e.g. APRL/2025/0469" required>
            </div>

            <div class="form-group">
              <label class="form-label">Target Username</label>
              <input type="text" id="transNewUser" class="form-input" placeholder="e.g. jdoe@example.com" required>
            </div>

            <div class="form-checkbox-label">
              <input type="checkbox" id="transDryRun" checked>
              <span>Dry Run Mode (Preview only, no DB writes)</span>
            </div>

            <div style="display: flex; gap: 0.5rem;">
              <button type="button" class="btn btn-secondary" onclick="previewTransfer()">
                👁️ Preview Changes
              </button>
              <button type="submit" class="btn btn-warning" id="transBtn">
                ⚡ Execute Transfer
              </button>
            </div>
          </form>

          <div class="status-box" id="transStatus"></div>
        </div>

        <!-- 3.2 Restore / Rollback Ownership Transfer -->
        <div class="step-card">
          <span class="step-badge badge-tertiary-order">RECOVERY</span>
          <div class="step-header">Rollback Ownership Transfer (Restore Snapshot)</div>
          <div class="step-details">
            Restores previous owner and created_by values from <code>public.transfer_ownership_backup</code>. Fully restores all child tables.
          </div>

          <form onsubmit="handleTransferRestore(event)">
            <div class="form-group">
              <label class="form-label">Application Number to Restore</label>
              <input type="text" id="restAppNo" class="form-input" placeholder="e.g. APRL/2025/0469" required>
            </div>

            <div class="form-checkbox-label">
              <input type="checkbox" id="restDryRun" checked>
              <span>Dry Run Mode (Inspect backup snapshot only)</span>
            </div>

            <button type="submit" class="btn btn-danger" id="restBtn">
              ↩️ Restore Original Owner
            </button>
          </form>

          <div class="status-box" id="restStatus"></div>
        </div>

        <!-- 3.3 Quick Single Transfer by Approval Number -->
        <div class="step-card">
          <span class="step-badge badge-tool">DIRECT TRANSFER</span>
          <div class="step-header">Transfer by Approval Number</div>
          <div class="step-details">
            Transfers application ownership matching approval number directly via <code>/api/v1/admin-tools/transfer-application</code> with audit log.
          </div>

          <form onsubmit="handleDirectTransfer(event)">
            <div class="form-group">
              <label class="form-label">Approval / License Number</label>
              <input type="text" id="dirAppNo" class="form-input" placeholder="e.g. PRL-2021-097" required>
            </div>
            <div class="form-group">
              <label class="form-label">Destination Username</label>
              <input type="text" id="dirNewUser" class="form-input" placeholder="e.g. target_operator" required>
            </div>
            <button type="submit" class="btn btn-primary" id="dirBtn">
              Reassign Application
            </button>
          </form>

          <div class="status-box" id="dirStatus"></div>
        </div>

        <!-- 3.4 Setup Institution / Company User -->
        <div class="step-card">
          <span class="step-badge badge-tool">INSTITUTION SETUP</span>
          <div class="step-header">Setup Institution User & Assign Role</div>
          <div class="step-details">
            Configures user as <code>INSTITUTION</code> / <code>COMPANY</code>, links them to a company UUID, and assigns designated role (e.g. <code>wasa_admin</code>).
          </div>

          <form onsubmit="handleSetupInstitution(event)">
            <div class="form-group">
              <label class="form-label">User UUID</label>
              <input type="text" id="instUserId" class="form-input" placeholder="e.g. 550e8400-e29b-41d4-a716-446655440000" required>
            </div>
            <div class="form-group">
              <label class="form-label">Company UUID</label>
              <input type="text" id="instCompId" class="form-input" placeholder="e.g. 6ba7b810-9dad-11d1-80b4-00c04fd430c8" required>
            </div>
            <div class="form-group">
              <label class="form-label">Role Name</label>
              <input type="text" id="instRole" class="form-input" value="wasa_admin" required>
            </div>
            <button type="submit" class="btn btn-primary" id="instBtn">
              Configure Institution User
            </button>
          </form>

          <div class="status-box" id="instStatus"></div>
        </div>

      </div>

    </div>

    <!-- ========================================== -->
    <!-- TAB 4: SCHEMA, INTEGRITY & BACKFILL TOOLS  -->
    <!-- ========================================== -->
    <div id="maintenanceTab" class="tab-content">

      <div class="first-timer-tip">
        <div>🛡️</div>
        <div>
          <strong>Safety First:</strong> Tools that modify database records provide a <strong>Dry Run</strong> option.
          Preview the row counts first before committing live updates.
        </div>
      </div>

      <!-- SECTION 1: CERTIFICATES INTEGRITY -->
      <div class="section-headline">
        <span>1. Certificate Deduplication & Alignment</span>
      </div>

      <div class="steps-grid">

        <!-- 4.1 Fix Certificates -->
        <div class="step-card">
          <span class="step-badge badge-primary-order">CORE CERT TOOL</span>
          <div class="step-header">Fix & Deduplicate Certificates</div>
          <div class="step-details">
            Enforces 1:1 certificate relationship per approval number: deletes duplicate rows, syncs latest dates from approved applications, backfills intimate reminder dates, and re-links foreign keys.
          </div>
          <button class="btn btn-primary" onclick="callAdminApi('/api/v1/admin-tools/fix-certificates', 'POST', null, 'fixCertStatus', 'Certificates Deduplication')">
            ⚡ Run Certificate Deduplication & Sync
          </button>
          <div class="status-box" id="fixCertStatus"></div>
        </div>

        <!-- 4.2 Sync Certificate Owner -->
        <div class="step-card">
          <span class="step-badge badge-tool">CERT OWNER</span>
          <div class="step-header">Sync Certificate Owner Names</div>
          <div class="step-details">
            Populates missing <code>certificate_owner</code> across all certificates using facility name + P.O. Box for sector apps, and applicant name for electrical installations.
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/sync-certificate-owner?db_env=' + getEnv(), 'POST', null, 'syncOwnerStatus', 'Certificate Owner Sync')">
            Sync Certificate Owner Names
          </button>
          <div class="status-box" id="syncOwnerStatus"></div>
        </div>

        <!-- 4.3 Fix Owner ID -->
        <div class="step-card">
          <span class="step-badge badge-tool">CERT OWNER ID</span>
          <div class="step-header">Fix Certificate Owner ID (UUID)</div>
          <div class="step-details">
            Backfills NULL <code>certificates.owner_id</code> from parent <code>applications.created_by</code> where a valid registered user exists.
          </div>
          <div class="form-checkbox-label">
            <input type="checkbox" id="fixOwnerDryRun" checked>
            <span>Dry Run Mode</span>
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/fix-owner-id?dry_run=' + document.getElementById('fixOwnerDryRun').checked, 'POST', null, 'fixOwnerStatus', 'Fix Owner ID')">
            Execute Owner ID Fix
          </button>
          <div class="status-box" id="fixOwnerStatus"></div>
        </div>

      </div>

      <!-- SECTION 2: GEOGRAPHY & LOCATION -->
      <div class="section-headline">
        <span>2. Geographical Location & Zone Fixes</span>
      </div>

      <div class="steps-grid">

        <!-- 4.4 Fix Region Zones -->
        <div class="step-card">
          <span class="step-badge badge-tool">ZONES</span>
          <div class="step-header">Fix Region & Zones Mapping</div>
          <div class="step-details">
            Cleans UUID region strings in <code>contact_details</code> to proper names, and dynamically backfills <code>zone_id</code> and <code>zone_name</code> on all applications.
          </div>
          <div class="form-checkbox-label">
            <input type="checkbox" id="fixZonesDryRun" checked>
            <span>Dry Run Mode</span>
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/fix-region-zones?dry_run=' + document.getElementById('fixZonesDryRun').checked, 'POST', null, 'fixZonesStatus', 'Region Zones Fix')">
            Run Region & Zone Alignment
          </button>
          <div class="status-box" id="fixZonesStatus"></div>
        </div>

        <!-- 4.5 Backfill Location Fields -->
        <div class="step-card">
          <span class="step-badge badge-tool">LOCATIONS</span>
          <div class="step-header">Backfill Missing Location Fields</div>
          <div class="step-details">
            Copies missing region, district, street, and ward into <code>applications</code> from <code>application_sector_details</code> wherever NULL.
          </div>
          <div class="form-checkbox-label">
            <input type="checkbox" id="backfillLocDryRun" checked>
            <span>Dry Run Mode</span>
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/backfill-location?dry_run=' + document.getElementById('backfillLocDryRun').checked, 'POST', null, 'backfillLocStatus', 'Backfill Location')">
            Backfill Location Fields
          </button>
          <div class="status-box" id="backfillLocStatus"></div>
        </div>

        <!-- 4.6 Update Zone ID -->
        <div class="step-card">
          <span class="step-badge badge-tool">ZONE ID</span>
          <div class="step-header">Update Zone ID on Applications</div>
          <div class="step-details">
            Re-evaluates <code>napa_regions</code> to update missing <code>applications.zone_id</code>.
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/update-zone-id', 'POST', null, 'updateZoneStatus', 'Update Zone ID')">
            Update Applications Zone IDs
          </button>
          <div class="status-box" id="updateZoneStatus"></div>
        </div>

      </div>

      <!-- SECTION 3: USERS & CHILD TABLES INTEGRITY -->
      <div class="section-headline">
        <span>3. User Identity & Relational Integrity</span>
      </div>

      <div class="steps-grid">

        <!-- 4.7 Repair & Backfill created_by -->
        <div class="step-card">
          <span class="step-badge badge-primary-order">USER REPAIR</span>
          <div class="step-header">Repair Missing Users & Backfill created_by</div>
          <div class="step-details">
            Detects usernames in applications that don't exist in <code>users</code> table, auto-creates active user records, assigns default roles, and backfills <code>created_by</code> across all child tables.
          </div>
          <button class="btn btn-primary" onclick="callAdminApi('/api/v1/admin-tools/repair-and-backfill', 'POST', null, 'repairUsersStatus', 'Repair Missing Users')">
            ⚡ Run Repair & Backfill created_by
          </button>
          <div class="status-box" id="repairUsersStatus"></div>
        </div>

        <!-- 4.8 Backfill application_id -->
        <div class="step-card">
          <span class="step-badge badge-tool">CHILD TABLES</span>
          <div class="step-header">Backfill application_id on Child Tables</div>
          <div class="step-details">
            Propagates <code>application_id</code> foreign key across child tables (shareholders, directors, documents, etc.) where missing.
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/backfill-application-id', 'POST', null, 'bfAppIdStatus', 'Backfill application_id')">
            Backfill Child Tables application_id
          </button>
          <div class="status-box" id="bfAppIdStatus"></div>
        </div>

        <!-- 4.9 Clean Name Fields -->
        <div class="step-card">
          <span class="step-badge badge-tool">SANITIZATION</span>
          <div class="step-header">Clean Phone & Emails in Name Fields</div>
          <div class="step-details">
            Extracts phone numbers (+255...) and emails embedded inside name columns into proper <code>mobile_no</code> and <code>email</code> fields in supervisor and custom details.
          </div>
          <div class="form-checkbox-label">
            <input type="checkbox" id="cleanNameDryRun" checked>
            <span>Dry Run Mode</span>
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/clean-name-fields?dry_run=' + document.getElementById('cleanNameDryRun').checked, 'POST', null, 'cleanNameStatus', 'Clean Name Fields')">
            Clean Name Fields
          </button>
          <div class="status-box" id="cleanNameStatus"></div>
        </div>

      </div>

      <!-- SECTION 4: LICENSE TYPES & SCHEMA -->
      <div class="section-headline">
        <span>4. License Type Rules & Database Schema</span>
      </div>

      <div class="steps-grid">

        <!-- 4.10 Sync License Types -->
        <div class="step-card">
          <span class="step-badge badge-tool">LICENSE TYPES</span>
          <div class="step-header">Sync License Types & Operational Categories</div>
          <div class="step-details">
            Updates <code>license_type</code> and <code>category_license_type</code> across applications and certificates matching sector rules.
          </div>
          <div class="form-checkbox-label">
            <input type="checkbox" id="syncTypeDryRun" checked>
            <span>Dry Run Mode</span>
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/sync-license-type?dry_run=' + document.getElementById('syncTypeDryRun').checked, 'POST', null, 'syncTypeStatus', 'Sync License Types')">
            Sync License Types
          </button>
          <div class="status-box" id="syncTypeStatus"></div>
        </div>

        <!-- 4.11 Update Natural Gas Category ID -->
        <div class="step-card">
          <span class="step-badge badge-tool">NATURAL GAS</span>
          <div class="step-header">Update Natural Gas Category ID</div>
          <div class="step-details">
            Backfills <code>applications.category_id</code> from <code>natural_gas_mapping</code> table.
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/update-natural-gas-category-id', 'POST', null, 'ngCatStatus', 'Natural Gas Category ID')">
            Update Natural Gas Category IDs
          </button>
          <div class="status-box" id="ngCatStatus"></div>
        </div>

        <!-- 4.12 Sync Database Schemas -->
        <div class="step-card">
          <span class="step-badge badge-tool">DDL SCHEMA</span>
          <div class="step-header">Sync Live Database Schema</div>
          <div class="step-details">
            Applies schema alignment script (<code>align_live_schema_eservice_construction.sql</code>) to ensure all columns, constraints, and tables exist.
          </div>
          <div class="form-checkbox-label">
            <input type="checkbox" id="syncSchemaDryRun" checked>
            <span>Dry Run Mode</span>
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/sync-schemas?dry_run=' + document.getElementById('syncSchemaDryRun').checked, 'POST', null, 'syncSchemaStatus', 'Sync Schemas')">
            Align Database Schema
          </button>
          <div class="status-box" id="syncSchemaStatus"></div>
        </div>

      </div>

    </div>

    <!-- ========================================== -->
    <!-- TAB 5: REFERENCE DATA & DIAGNOSTICS        -->
    <!-- ========================================== -->
    <div id="referenceTab" class="tab-content">

      <div class="card">
        <div class="card-header">
          <div>
            <div class="card-title">📚 Interactive Reference Data Explorers</div>
            <div class="card-desc">Inspect active system categories, roles, and administrative configuration in real-time.</div>
          </div>
          <div>
            <button class="btn btn-secondary" style="width: auto; padding: 0.4rem 0.8rem;" onclick="callAdminApi('/api/v1/admin-tools/refresh-cache?target=all', 'POST', null, 'cacheStatus', 'Cache Refresh')">
              🔄 Refresh Cache
            </button>
          </div>
        </div>
        <div class="status-box" id="cacheStatus"></div>

        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 1.5rem; margin-top: 1rem;">

          <!-- 5.1 Categories Explorer -->
          <div class="step-card">
            <div class="step-header">System Categories Explorer</div>
            <div class="step-details">Live searchable index of all license categories in EWURA LOIS.</div>
            <div style="display: flex; gap: 0.5rem; margin-bottom: 0.75rem;">
              <input type="text" id="catSearchInput" class="form-input" placeholder="Search category name or sector..." onkeydown="if(event.key==='Enter') loadCategories()">
              <button class="btn btn-primary" style="width: auto;" onclick="loadCategories()">Search</button>
            </div>
            <div class="table-responsive">
              <table class="data-table" id="categoriesTable">
                <thead>
                  <tr>
                    <th>Category Name</th>
                    <th>Type</th>
                    <th>Sector</th>
                    <th>ID</th>
                  </tr>
                </thead>
                <tbody id="categoriesTbody">
                  <tr><td colspan="4" style="text-align:center; color: #94a3b8;">Click 'Search' or load categories</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- 5.2 Roles Explorer -->
          <div class="step-card">
            <div class="step-header">System Roles & User Counts</div>
            <div class="step-details">Live index of all security roles in LOIS and count of users assigned to each.</div>
            <div style="display: flex; gap: 0.5rem; margin-bottom: 0.75rem;">
              <input type="text" id="roleSearchInput" class="form-input" placeholder="Search role name..." onkeydown="if(event.key==='Enter') loadRoles()">
              <button class="btn btn-primary" style="width: auto;" onclick="loadRoles()">Search</button>
            </div>
            <div class="table-responsive">
              <table class="data-table" id="rolesTable">
                <thead>
                  <tr>
                    <th>Role Name</th>
                    <th>Assigned Users</th>
                    <th>Role ID</th>
                  </tr>
                </thead>
                <tbody id="rolesTbody">
                  <tr><td colspan="3" style="text-align:center; color: #94a3b8;">Click 'Search' or load roles</td></tr>
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>

      <!-- SECTION 5B: SPECIALIZED ADMIN UTILITIES -->
      <div class="section-headline">
        <span>Administrative Configuration & Troubleshooting</span>
      </div>

      <div class="steps-grid">

        <!-- Copy Category Attachment Config -->
        <div class="step-card">
          <span class="step-badge badge-tool">CONFIG COPY</span>
          <div class="step-header">Copy Category Attachment Config</div>
          <div class="step-details">
            Clones all document attachment rules from a source category to a destination category idempotently.
          </div>
          <form onsubmit="handleCopyCatConfig(event)">
            <div class="form-group">
              <label class="form-label">Source Category ID (UUID)</label>
              <input type="text" id="copySrcId" class="form-input" placeholder="Source category UUID" required>
            </div>
            <div class="form-group">
              <label class="form-label">Destination Category ID (UUID)</label>
              <input type="text" id="copyDestId" class="form-input" placeholder="Destination category UUID" required>
            </div>
            <button type="submit" class="btn btn-secondary" id="copyCatBtn">
              Copy Attachment Rules
            </button>
          </form>
          <div class="status-box" id="copyCatStatus"></div>
        </div>

        <!-- Assign Default Role -->
        <div class="step-card">
          <span class="step-badge badge-tool">ROLE ASSIGN</span>
          <div class="step-header">Assign DEFAULT Role to Applicants</div>
          <div class="step-details">
            Finds all users who hold 'APPLICANT ROLE' but lack 'DEFAULT' role, and assigns it automatically.
          </div>
          <button class="btn btn-secondary" onclick="callAdminApi('/api/v1/admin-tools/assign-default-role', 'POST', null, 'assignRoleStatus', 'Assign Default Role')">
            Assign Missing DEFAULT Role
          </button>
          <div class="status-box" id="assignRoleStatus"></div>
        </div>

        <!-- Password Troubleshooter -->
        <div class="step-card">
          <span class="step-badge badge-warning">TROUBLESHOOTING</span>
          <div class="step-header">Password Troubleshooter (Test Access)</div>
          <div class="step-details">
            Temporarily backs up a locked user's password hash and replaces it with an admin hash for testing, with 1-click restore.
          </div>
          <form onsubmit="handlePasswordTroubleshoot(event)">
            <div class="form-group">
              <label class="form-label">Problem Username</label>
              <input type="text" id="passProblemUser" class="form-input" placeholder="e.g. applicant.john" required>
            </div>
            <div class="form-group">
              <label class="form-label">Admin Username</label>
              <input type="text" id="passAdminUser" class="form-input" value="mkulasi.robert" required>
            </div>
            <div style="display: flex; gap: 0.5rem;">
              <button type="button" class="btn btn-warning" onclick="startPasswordTroubleshoot()">
                Start Test Access
              </button>
              <button type="button" class="btn btn-secondary" onclick="restorePasswordTroubleshoot()">
                Restore Original
              </button>
            </div>
          </form>
          <div class="status-box" id="passTroubleStatus"></div>
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

      // Auto-load reference data if switching to reference tab
      if (tabId === 'referenceTab') {
        loadCategories();
        loadRoles();
      }
    }

    // Keep global environment synced across entire app
    function syncEnv(val) {
      const select = document.getElementById('globalEnv');
      if (select) select.value = val;
      const badge = document.getElementById('headerEnvBadge');
      if (badge) {
        badge.className = 'env-badge env-' + val;
        badge.innerText = val.toUpperCase() + (val === 'prod' ? ' (10.1.8.166)' : '') + ' Active';
      }
    }

    function getEnv() {
      const select = document.getElementById('globalEnv');
      return select ? select.value : 'prod';
    }

    function showChosenFile(input, targetId) {
      const label = document.getElementById(targetId);
      if (input.files && input.files[0]) {
        label.innerText = '✓ Selected: ' + input.files[0].name + ' (' + (input.files[0].size / 1024).toFixed(1) + ' KB)';
      } else {
        label.innerText = '';
      }
    }

    // ==========================================
    // UPLOAD PIPELINES HANDLERS
    // ==========================================

    // 1. Sector Applications (Petroleum, Natural Gas, Electricity, Water)
    async function handleSectorAppUpload(e) {
      e.preventDefault();
      const fileInput = document.getElementById('appFile');
      const sector = document.getElementById('appSector').value;
      const dbEnv = getEnv();
      const btn = document.getElementById('appSubmitBtn');
      const statusBox = document.getElementById('appStatus');

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + fileInput.files[0].name + ' to ' + dbEnv.toUpperCase() + '...';

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
          pollStatus('/api/v1/application-migrations/status/', data.job_id, statusBox, btn, 'Upload Applications File');
        } else if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ Successfully Processed!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
          btn.disabled = false;
          btn.innerText = 'Upload Applications File';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Upload failed (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
          btn.disabled = false;
          btn.innerText = 'Upload Applications File';
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
        btn.disabled = false;
        btn.innerText = 'Upload Applications File';
      }
    }

    // 2. Water Supply Specialized Upload
    async function handleWaterSupplyUpload(e) {
      e.preventDefault();
      const fileInput = document.getElementById('waterFile');
      const dbEnv = getEnv();
      const btn = document.getElementById('waterSubmitBtn');
      const statusBox = document.getElementById('waterStatus');

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading Water Supply...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + fileInput.files[0].name + ' to ' + dbEnv.toUpperCase() + '...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);
      formData.append('db_env', dbEnv);
      formData.append('background', 'true');

      try {
        const resp = await fetch('/api/v1/water-supply/upload', {
          method: 'POST',
          body: formData
        });
        const data = await resp.json();
        if (resp.status === 202) {
          statusBox.innerHTML = '<strong>✓ Water Supply Job Accepted!</strong> Job ID: <code>' + data.job_id + '</code>.<br>Processing records...';
          pollStatus('/api/v1/water-supply/status/', data.job_id, statusBox, btn, 'Upload Water Applications');
        } else if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ Completed!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
          btn.disabled = false;
          btn.innerText = 'Upload Water Applications';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Upload failed: ' + (data.detail || JSON.stringify(data));
          btn.disabled = false;
          btn.innerText = 'Upload Water Applications';
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
        btn.disabled = false;
        btn.innerText = 'Upload Water Applications';
      }
    }

    // 3. Electrical Installation Pipeline Uploads
    async function handleElectricalPipelineUpload(e, url, fileInputId, btnId, statusId, label) {
      e.preventDefault();
      const fileInput = document.getElementById(fileInputId);
      const dbEnv = getEnv();
      const btn = document.getElementById(btnId);
      const statusBox = document.getElementById(statusId);

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + label + ' to ' + dbEnv.toUpperCase() + '...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);
      formData.append('db_env', dbEnv);
      formData.append('sync', 'true');
      formData.append('background', 'false');

      try {
        const resp = await fetch(url, {
          method: 'POST',
          body: formData
        });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ ' + label + ' Processed!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = label + ' error (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Upload ' + label;
      }
    }

    // 4. Generic Form File Upload (Shareholders, Directors)
    async function handleGenericFormUpload(e, url, fileInputId, btnId, statusBoxId, label) {
      e.preventDefault();
      const fileInput = document.getElementById(fileInputId);
      const dbEnv = getEnv();
      const btn = document.getElementById(btnId);
      const statusBox = document.getElementById(statusBoxId);

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + label + ' to ' + dbEnv.toUpperCase() + '...';

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
          statusBox.innerHTML = '<strong>✓ ' + label + ' Upload Successful!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = label + ' error (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Upload ' + label;
      }
    }

    // 5. Direct File Upload (LOIS Users, Categories)
    async function handleDirectFileUpload(e, url, fileInputId, btnId, statusBoxId, label) {
      e.preventDefault();
      const fileInput = document.getElementById(fileInputId);
      const btn = document.getElementById(btnId);
      const statusBox = document.getElementById(statusBoxId);

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading ' + label + '...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);

      try {
        const resp = await fetch(url, {
          method: 'POST',
          body: formData
        });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ ' + label + ' Imported Successfully!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Upload failed (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Upload ' + label;
      }
    }

    // 6. License Categories & Fees Upload
    async function handleLicenseCategoryFeesUpload(e) {
      e.preventDefault();
      const fileInput = document.getElementById('licCatFile');
      const sector = document.getElementById('licCatSector').value;
      const dbEnv = getEnv();
      const btn = document.getElementById('licCatBtn');
      const statusBox = document.getElementById('licCatStatus');

      if (!fileInput.files[0]) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Uploading...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Uploading License Categories for sector ' + sector + '...';

      const formData = new FormData();
      formData.append('file', fileInput.files[0]);
      formData.append('db_env', dbEnv);

      try {
        const fullUrl = '/api/v1/license-categories/upload?sector_name=' + encodeURIComponent(sector) + '&sync=true&background=false';
        const resp = await fetch(fullUrl, {
          method: 'POST',
          body: formData
        });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ License Categories Imported!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Upload failed: ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Upload License Category Fees';
      }
    }

    // Generic Job Status Poller
    async function pollStatus(endpointPrefix, jobId, statusBox, btn, btnLabel) {
      let attempts = 0;
      const maxAttempts = 60;
      const interval = setInterval(async () => {
        attempts++;
        try {
          const res = await fetch(endpointPrefix + jobId);
          if (res.ok) {
            const st = await res.json();
            if (st.status === 'COMPLETED') {
              clearInterval(interval);
              statusBox.className = 'status-box status-success';
              statusBox.innerHTML = '<strong>🎉 Ingestion Completed Successfully!</strong><br><pre class="json-view">' + JSON.stringify(st.result || st, null, 2) + '</pre>';
              btn.disabled = false;
              btn.innerText = btnLabel;
            } else if (st.status === 'FAILED') {
              clearInterval(interval);
              statusBox.className = 'status-box status-error';
              statusBox.innerHTML = '<strong>❌ Processing Failed:</strong> ' + (st.error || 'Unknown error');
              btn.disabled = false;
              btn.innerText = btnLabel;
            } else {
              statusBox.innerHTML = '<strong>Processing in progress...</strong> ' + (st.progress || 'Running...') + ' (Check ' + attempts + ')';
            }
          }
        } catch (e) {
          // ignore transient network poll error
        }
        if (attempts >= maxAttempts) {
          clearInterval(interval);
          statusBox.innerHTML += '<br><em>Job still processing in background. You can check Swagger status later.</em>';
          btn.disabled = false;
          btn.innerText = btnLabel;
        }
      }, 2500);
    }

    // ==========================================
    // LOOKUP & RECORD INSPECTOR
    // ==========================================
    function setAndSearch(num) {
      document.getElementById('lookupNum').value = num;
      executeLookup();
    }

    async function executeLookup() {
      const numInput = document.getElementById('lookupNum');
      const val = numInput.value.trim();
      const env = getEnv();
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

    // ==========================================
    // OWNERSHIP TRANSFER HANDLERS
    // ==========================================
    async function previewTransfer() {
      const appNo = document.getElementById('transAppNo').value.trim();
      const newUser = document.getElementById('transNewUser').value.trim();
      const statusBox = document.getElementById('transStatus');

      if (!appNo || !newUser) {
        alert('Please enter both Application Number and Target Username');
        return;
      }

      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Fetching transfer preview...';

      try {
        const url = '/api/v1/admin-tools/transfer-ownership/preview?application_number=' + encodeURIComponent(appNo) + '&new_username=' + encodeURIComponent(newUser) + '&db_env=' + getEnv();
        const resp = await fetch(url);
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>👁️ Preview Ready (No Changes Made):</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Preview failed: ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      }
    }

    async function handleTransferSubmit(e) {
      e.preventDefault();
      const appNo = document.getElementById('transAppNo').value.trim();
      const newUser = document.getElementById('transNewUser').value.trim();
      const dryRun = document.getElementById('transDryRun').checked;
      const statusBox = document.getElementById('transStatus');
      const btn = document.getElementById('transBtn');

      if (!dryRun) {
        if (!confirm('Are you sure you want to transfer ownership of application ' + appNo + ' to ' + newUser + '? A backup snapshot will be saved.')) {
          return;
        }
      }

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Transferring...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Processing transfer...';

      try {
        const url = '/api/v1/admin-tools/transfer-ownership?application_number=' + encodeURIComponent(appNo) + '&new_username=' + encodeURIComponent(newUser) + '&dry_run=' + dryRun + '&db_env=' + getEnv();
        const resp = await fetch(url, { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ ' + (dryRun ? 'Dry Run Preview:' : 'Ownership Transfer Complete!') + '</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Transfer failed: ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = '⚡ Execute Transfer';
      }
    }

    async function handleTransferRestore(e) {
      e.preventDefault();
      const appNo = document.getElementById('restAppNo').value.trim();
      const dryRun = document.getElementById('restDryRun').checked;
      const statusBox = document.getElementById('restStatus');
      const btn = document.getElementById('restBtn');

      if (!dryRun) {
        if (!confirm('Are you sure you want to restore application ' + appNo + ' to its backed-up owner?')) {
          return;
        }
      }

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Restoring...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';
      statusBox.innerText = 'Restoring snapshot...';

      try {
        const url = '/api/v1/admin-tools/transfer-ownership/restore?application_number=' + encodeURIComponent(appNo) + '&dry_run=' + dryRun + '&db_env=' + getEnv();
        const resp = await fetch(url, { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ ' + (dryRun ? 'Backup Snapshot Found:' : 'Ownership Restored Successfully!') + '</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Restore failed: ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = '↩️ Restore Original Owner';
      }
    }

    async function handleDirectTransfer(e) {
      e.preventDefault();
      const appNo = document.getElementById('dirAppNo').value.trim();
      const destUser = document.getElementById('dirNewUser').value.trim();
      const env = getEnv();
      const statusBox = document.getElementById('dirStatus');
      const btn = document.getElementById('dirBtn');

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Reassigning...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';

      try {
        const url = '/api/v1/admin-tools/transfer-application?approval_no=' + encodeURIComponent(appNo) + '&destination_username=' + encodeURIComponent(destUser) + '&db_env=' + env;
        const resp = await fetch(url, { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ Application Reassigned!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Failed: ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Reassign Application';
      }
    }

    async function handleSetupInstitution(e) {
      e.preventDefault();
      const userId = document.getElementById('instUserId').value.trim();
      const compId = document.getElementById('instCompId').value.trim();
      const role = document.getElementById('instRole').value.trim();
      const env = getEnv();
      const statusBox = document.getElementById('instStatus');
      const btn = document.getElementById('instBtn');

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Configuring...';
      statusBox.style.display = 'block';
      statusBox.className = 'status-box status-info';

      try {
        const url = '/api/v1/admin-tools/setup-institution-user?user_id=' + encodeURIComponent(userId) + '&company_id=' + encodeURIComponent(compId) + '&role_name=' + encodeURIComponent(role) + '&db_env=' + env;
        const resp = await fetch(url, { method: 'POST' });
        const data = await resp.json();
        if (resp.ok) {
          statusBox.className = 'status-box status-success';
          statusBox.innerHTML = '<strong>✓ Institution User Configured!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          statusBox.className = 'status-box status-error';
          statusBox.innerText = 'Setup failed: ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusBox.className = 'status-box status-error';
        statusBox.innerText = 'Network error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerText = 'Configure Institution User';
      }
    }

    // ==========================================
    // GENERIC ADMIN TOOLS DISPATCHER
    // ==========================================
    async function callAdminApi(url, method, body, statusBoxId, label) {
      const box = document.getElementById(statusBoxId);
      box.style.display = 'block';
      box.className = 'status-box status-info';
      box.innerText = 'Executing ' + label + '...';

      try {
        const opts = { method: method || 'POST' };
        if (body) {
          opts.headers = { 'Content-Type': 'application/json' };
          opts.body = JSON.stringify(body);
        }
        const resp = await fetch(url, opts);
        const data = await resp.json();
        if (resp.ok) {
          box.className = 'status-box status-success';
          box.innerHTML = '<strong>✓ ' + label + ' Success!</strong><br><pre class="json-view">' + JSON.stringify(data, null, 2) + '</pre>';
        } else {
          box.className = 'status-box status-error';
          box.innerText = 'Error (' + resp.status + '): ' + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        box.className = 'status-box status-error';
        box.innerText = 'Request failed: ' + err.message;
      }
    }

    // ==========================================
    // REFERENCE DATA: CATEGORIES & ROLES
    // ==========================================
    async function loadCategories() {
      const search = document.getElementById('catSearchInput').value.trim();
      const tbody = document.getElementById('categoriesTbody');
      tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;">Loading categories...</td></tr>';

      try {
        const url = '/api/v1/admin-tools/list-categories?search=' + encodeURIComponent(search);
        const resp = await fetch(url);
        const data = await resp.json();
        if (resp.ok && data.options) {
          if (data.options.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color:#94a3b8;">No matching categories found</td></tr>';
            return;
          }
          tbody.innerHTML = data.options.map(c => `
            <tr>
              <td><strong>${c.label || '-'}</strong></td>
              <td><span class="badge badge-license">${c.category_type || 'License'}</span></td>
              <td>${c.sector || '-'}</td>
              <td><code style="font-size:0.75rem;">${c.value}</code></td>
            </tr>
          `).join('');
        } else {
          tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color:red;">Failed to load categories</td></tr>';
        }
      } catch (e) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color:red;">Error loading: ' + e.message + '</td></tr>';
      }
    }

    async function loadRoles() {
      const search = document.getElementById('roleSearchInput').value.trim();
      const tbody = document.getElementById('rolesTbody');
      tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;">Loading roles...</td></tr>';

      try {
        const url = '/api/v1/admin-tools/list-roles?search=' + encodeURIComponent(search);
        const resp = await fetch(url);
        const data = await resp.json();
        if (resp.ok && data.options) {
          if (data.options.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:#94a3b8;">No matching roles found</td></tr>';
            return;
          }
          tbody.innerHTML = data.options.map(r => `
            <tr>
              <td><strong>${r.label || '-'}</strong></td>
              <td><span class="badge badge-approved">${r.user_count !== undefined ? r.user_count : '-'} users</span></td>
              <td><code style="font-size:0.75rem;">${r.value}</code></td>
            </tr>
          `).join('');
        } else {
          tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:red;">Failed to load roles</td></tr>';
        }
      } catch (e) {
        tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:red;">Error loading: ' + e.message + '</td></tr>';
      }
    }

    async function handleCopyCatConfig(e) {
      e.preventDefault();
      const src = document.getElementById('copySrcId').value.trim();
      const dest = document.getElementById('copyDestId').value.trim();
      callAdminApi('/api/v1/admin-tools/copy-category-config?source_category_id=' + encodeURIComponent(src) + '&destination_category_id=' + encodeURIComponent(dest), 'POST', null, 'copyCatStatus', 'Copy Category Config');
    }

    async function startPasswordTroubleshoot() {
      const prob = document.getElementById('passProblemUser').value.trim();
      const admin = document.getElementById('passAdminUser').value.trim();
      if (!prob) { alert('Enter Problem Username'); return; }
      callAdminApi('/api/v1/admin-tools/password-troubleshoot/start?problem_username=' + encodeURIComponent(prob) + '&admin_username=' + encodeURIComponent(admin), 'POST', null, 'passTroubleStatus', 'Start Password Troubleshoot');
    }

    async function restorePasswordTroubleshoot() {
      const prob = document.getElementById('passProblemUser').value.trim();
      const admin = document.getElementById('passAdminUser').value.trim();
      if (!prob) { alert('Enter Problem Username'); return; }
      callAdminApi('/api/v1/admin-tools/password-troubleshoot/restore?problem_username=' + encodeURIComponent(prob) + '&admin_username=' + encodeURIComponent(admin), 'POST', null, 'passTroubleStatus', 'Restore Original Password');
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
