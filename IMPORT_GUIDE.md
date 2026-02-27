# EWURA Migration API: Step-by-Step Data Import Guide

This guide explains how to run all major data impor## 10. General Tips

- Always check API responses for errors or warnings.
- Use `include_rows` and `limit_rows` parameters to preview imported rows if needed.
- For large files, use background job endpoints if available.
- Review terminal logs for progress and error details.

---

## 11. Troubleshootinggement operations in the ewura-migration FastAPI project. Follow these steps to upload sector data, manage directors/shareholders, backfill users, and clean name fields.

---

## 1. Environment Setup

- Set the environment (test, staging, or production):
  ```sh
  ./use-env.sh test
  ```
  (Replace `test` as needed.)
- Restart the API server after switching environments.

## 2. Start the API Server

- Run:
  ```sh
  uvicorn app.main:app --reload
  ```
- Open Swagger UI at: http://localhost:8000/docs

---

## 3. Upload Users (First Step)

Before uploading any sector data, you must upload the users to ensure that applications and other records can be properly linked to their respective owners.

- Endpoint: `POST /api/v1/users_upload` (or the relevant users upload endpoint)
- Upload the main users Excel/CSV file.
- Wait for the import to complete and verify that users are successfully created in the database.

---

## 4. Upload Sector Data

### Petroleum Sector
- Endpoint: `POST /api/v1/petroleum_upload`
- Upload the petroleum Excel/CSV file.

### Natural Gas Sector
- Endpoint: `POST /api/v1/natural_gas_upload`
- Upload the natural gas file.

### Electricity Sector
- Endpoint: `POST /api/v1/electricity_upload`
- Upload the electricity file.

### Electrical Installation (Main)
- Endpoint: `POST /api/v1/electrical_installations_upload`
- Upload the main installation file.

### Electrical Installation – Employed
- Endpoint: `POST /api/v1/electrical_installations_employed_upload`
- Upload the employed file.

### Electrical Installation – Self Employed
- Endpoint: `POST /api/v1/electrical_installations_self_employed_upload`
- Upload the self-employed file.

### Electrical Installation – Certificate Verification
- Endpoint: `POST /api/v1/electrical_certificate_verifications_upload`
- Upload the certificate verification file.

---

## 5. Managing Directors Upload

- Petroleum: `POST /api/v1/petroleum_managing_directors_upload`
- Electricity: `POST /api/v1/electricity_managing_directors_upload`
- Natural Gas: `POST /api/v1/natural_gas_managing_directors_upload`
- Upload the relevant file for each sector.

---

## 6. Shareholders Upload

- Petroleum: `POST /api/v1/petroleum_shareholders_upload`
- Electricity: `POST /api/v1/electricity_shareholders_upload`
- Natural Gas: `POST /api/v1/natural_gas_shareholders_upload`
- Upload the relevant file for each sector.

---

## 7. Supervisors Upload

- Endpoint: `POST /api/v1/electrical_installations_supervisors_upload`
- Upload the supervisors file.

---

## 8. Backfill Users

- Endpoint: `POST /api/v1/backfill_users`
- Run this after all main uploads to backfill user data.

---

## 9. Clean Name Fields

- Endpoint: `POST /api/v1/clean_names`
- Use this to clean and normalize name fields across tables.

---

## 10. General Tips

- Always check API responses for errors or warnings.
- Use `include_rows` and `limit_rows` parameters to preview imported rows if needed.
- For large files, use background job endpoints if available.
- Review terminal logs for progress and error details.

---

## 10. Troubleshooting

- If you encounter errors, check the logs and ensure your files match the expected format.
- For DB connection issues, verify your `.env` and environment selection.

---

For more details, see the main `README.md` or contact the project maintainer.
