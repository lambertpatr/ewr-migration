# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.lois_users_upload import router as lois_users_router
from app.api.v1.application_migrations_upload import router as app_migrations_router
from app.api.v1.shareholders_upload import router as shareholders_router
from app.api.v1.managing_directors_upload import router as managing_directors_router
from app.api.v1.license_categories_upload import router as license_categories_router
from app.api.v1.admin_tools import router as admin_tools_router
from app.api.v1.electrical_installations_upload import router as electrical_installations_router
from app.api.v1.electrical_installations_supervisors_upload import (
	router as electrical_installations_supervisors_router,
)
from app.api.v1.electrical_installations_certificate_verifications_upload import (
	router as electrical_installations_cert_verifications_router,
)
from app.api.v1.sync_license_type import router as sync_license_type_router
import app.api.v1.categories_upload as categories_upload

app = FastAPI(title="EWURA LOIS Migration API")

# Add permissive CORS so Swagger UI Try-it works from the browser
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],  # restrict in production
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)

app.include_router(lois_users_router)
app.include_router(app_migrations_router)
app.include_router(shareholders_router)
app.include_router(managing_directors_router)
app.include_router(license_categories_router)
app.include_router(admin_tools_router)
app.include_router(electrical_installations_router)
app.include_router(electrical_installations_supervisors_router)
app.include_router(electrical_installations_cert_verifications_router)
app.include_router(categories_upload.router)
app.include_router(sync_license_type_router)
