# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.lois_users_upload import router as lois_users_router
from app.api.v1.application_migrations_upload import router as app_migrations_router
from app.api.v1.shareholders_upload import router as shareholders_router
from app.api.v1.managing_directors_upload import router as managing_directors_router
from app.api.v1.license_categories_upload import router as license_categories_router
from app.api.v1.admin_tools import router as admin_tools_router

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
