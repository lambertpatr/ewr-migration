from __future__ import annotations

"""Debug helper: validate licencetype values survive pandas->CSV->COPY path.

This script doesn't require FastAPI. It uses the same staging/COPY approach as
`import_license_categories_and_fees_via_staging_copy` but focuses on printing a
few staged values.

Usage: run inside the ewura-migration venv / env where DB URL is configured.
It expects SQLAlchemy engine config in app.core.database.
"""

import io

import pandas as pd
from sqlalchemy import text


def main():
    # minimal inline dataframe - replace with real file if needed
    df = pd.DataFrame(
        {
            "categoryorclass": ["Bulk Water Supply and Sewerage Treatment Services"],
            "licencetype": ["SOME_LICENSE"],
            "applicationtype": ["NEW"],
            "appfee": ["100"],
            "licencefee": ["200"],
            "prefix": ["A"],
            "licenseprefix": ["L"],
            "licenseperiod_x": ["36"],
        }
    )
    df.columns = [c.lower() for c in df.columns]

    from app.core import database as db_module

    db_module._init_engine()
    SessionLocal = db_module._SessionLocal

    db = SessionLocal()
    try:
        stage = "public._debug_stage_license"
        db.execute(
            text(
                f"""
                DROP TABLE IF EXISTS {stage};
                CREATE TABLE {stage} (
                    categoryorclass text,
                    licencetype text,
                    applicationtype text
                );
                """
            )
        )

        stage_df = pd.DataFrame(
            {
                "categoryorclass": df.get("categoryorclass"),
                "licencetype": df.get("licencetype"),
                "applicationtype": df.get("applicationtype"),
            }
        ).where(pd.notnull(df), None)

        buf = io.StringIO()
        stage_df.to_csv(buf, index=False, header=False)
        buf.seek(0)

        conn = db.connection().connection
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {stage} (categoryorclass, licencetype, applicationtype) FROM STDIN WITH (FORMAT CSV)",
                buf,
            )

        rows = db.execute(text(f"SELECT categoryorclass, licencetype, applicationtype FROM {stage} LIMIT 5")).all()
        print(rows)
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    main()
