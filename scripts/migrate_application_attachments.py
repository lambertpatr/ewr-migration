#!/usr/bin/env python3
"""
scripts/migrate_application_attachments.py

Bulk migration helper that uses the project's engine (via
`app.core.database.get_engine`) and migrates wide-attachment rows from the
legacy `applications` table into `ca_applications` and `ca_documents`.

Usage:
  python scripts/migrate_application_attachments.py --source-table applications
  --preview  # to run as a dry-run
"""
import argparse
import os
import sys

import io
import csv
import uuid
from sqlalchemy import text

from app.core.database import get_engine


def detect_attachment_pairs(cols):
    pairs = []
    filename_cols = [c for c in cols if c.lower().endswith('filename')]
    for fname in filename_cols:
        base = fname[:-len('filename')]
        id_col = None
        if base in cols:
            id_col = base
        elif (base + '_id') in cols:
            id_col = base + '_id'
        else:
            id_col = base if base in cols else None
        pairs.append((id_col, fname, base))
    return pairs


def migrate(engine, source_table='applications', batch_size=200, preview=False, fast_copy=False):
    if fast_copy:
        return migrate_fast_copy(engine, source_table=source_table, batch_size=batch_size, preview=preview)

    conn = engine.connect()
    trans = None
    try:
        q = text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = :t ORDER BY ordinal_position")
        cols = [r[0] for r in conn.execute(q, {"t": source_table}).fetchall()]
        if not cols:
            raise RuntimeError(f"source table '{source_table}' empty or not found")

        attachment_pairs = detect_attachment_pairs(cols)
        print(f"Detected {len(attachment_pairs)} attachment filename columns")

        excluded = set([fname for (_, fname, _) in attachment_pairs])
        excluded.update([base for (base, _, _) in attachment_pairs if base in cols])
        app_columns = [c for c in cols if c not in excluded]

        ca_cols = [r[0] for r in conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='ca_applications' ")).fetchall()]
        copy_cols = [c for c in app_columns if c in ca_cols and c != 'id']

        total = conn.execute(text(f"SELECT COUNT(*) FROM {source_table}")).scalar()
        print(f"Total rows: {total}")

        offset = 0
        inserted_apps = 0
        inserted_docs = 0

        while True:
            rows = conn.execute(text(f"SELECT * FROM {source_table} ORDER BY 1 OFFSET :off LIMIT :lim"), {"off": offset, "lim": batch_size}).mappings().all()
            if not rows:
                break

            if not preview:
                trans = conn.begin()

            app_inserts = []
            doc_inserts = []

            for r in rows:
                app_id = str(uuid.uuid4())
                app_row = {"id": app_id}
                for c in copy_cols:
                    app_row[c] = r.get(c)
                app_inserts.append(app_row)

                order = 1
                for id_col, filename_col, base in attachment_pairs:
                    logic_val = r.get(id_col) if id_col in r else None
                    fname_val = r.get(filename_col) if filename_col in r else None
                    if isinstance(fname_val, str) and fname_val.strip().lower() == 'nan':
                        fname_val = None
                    if (logic_val in (None, '') or str(logic_val).strip() == '') and (fname_val is None or (isinstance(fname_val, str) and fname_val.strip() == '')):
                        continue
                    doc_id = str(uuid.uuid4())
                    doc = {
                        "id": doc_id,
                        "document_name": fname_val,
                        "document_url": None,
                        "application_id": app_id,
                        "file_name": base,
                        "documents_order": order,
                        "logic_doc_id": int(logic_val) if logic_val not in (None, '') else None,
                    }
                    order += 1
                    doc_inserts.append(doc)

            if app_inserts:
                col_names = ["id"] + copy_cols
                cols_sql = ",".join(col_names)
                vals_sql = ",".join([f":{c}" for c in col_names])
                insert_sql = text(f"INSERT INTO ca_applications ({cols_sql}, created_at) VALUES ({vals_sql}, now())")
                if not preview:
                    conn.execute(insert_sql, app_inserts)
                inserted_apps += len(app_inserts)

            if doc_inserts:
                doc_cols = ["id", "document_name", "document_url", "application_id", "file_name", "documents_order", "logic_doc_id"]
                cols_sql = ",".join(doc_cols) + ", created_at"
                vals_sql = ",".join([f":{c}" for c in doc_cols]) + ", now()"
                insert_docs_sql = text(f"INSERT INTO ca_documents ({cols_sql}) VALUES ({vals_sql})")
                if not preview:
                    conn.execute(insert_docs_sql, doc_inserts)
                inserted_docs += len(doc_inserts)

            if not preview:
                trans.commit()
                trans = None

            offset += batch_size
            print(f"Processed offset {offset}: apps {inserted_apps}, docs {inserted_docs}")

        print("Done. Inserted apps:", inserted_apps, "docs:", inserted_docs)
    finally:
        if trans is not None:
            trans.rollback()
        conn.close()


def migrate_fast_copy(engine, source_table='applications', batch_size=200, preview=False):
    """Fast COPY-based migration per-batch. Generates UUIDs for apps/docs in Python,
    writes two in-memory CSVs, and uses COPY FROM STDIN for each table.
    """
    # Use a raw DBAPI connection for COPY
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        # Discover columns
        q = text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = :t ORDER BY ordinal_position")
        sa_conn = engine.connect()
        cols = [r[0] for r in sa_conn.execute(q, {"t": source_table}).fetchall()]
        if not cols:
            raise RuntimeError(f"source table '{source_table}' empty or not found")

        attachment_pairs = detect_attachment_pairs(cols)
        print(f"Detected {len(attachment_pairs)} attachment filename columns")

        excluded = set([fname for (_, fname, _) in attachment_pairs])
        excluded.update([base for (base, _, _) in attachment_pairs if base in cols])
        app_columns = [c for c in cols if c not in excluded]

        ca_cols = [r[0] for r in sa_conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='ca_applications' ")).fetchall()]
        copy_cols = [c for c in app_columns if c in ca_cols and c != 'id']

        total = sa_conn.execute(text(f"SELECT COUNT(*) FROM {source_table}")).scalar()
        print(f"Total rows: {total}")

        offset = 0
        inserted_apps = 0
        inserted_docs = 0

        while True:
            rows = sa_conn.execute(text(f"SELECT * FROM {source_table} ORDER BY 1 OFFSET :off LIMIT :lim"), {"off": offset, "lim": batch_size}).mappings().all()
            if not rows:
                break

            app_rows = []
            doc_rows = []

            for r in rows:
                app_id = str(uuid.uuid4())
                app_row = {"id": app_id}
                for c in copy_cols:
                    val = r.get(c)
                    app_row[c] = '' if val is None else str(val)
                app_rows.append(app_row)

                order = 1
                for id_col, filename_col, base in attachment_pairs:
                    logic_val = r.get(id_col) if id_col in r else None
                    fname_val = r.get(filename_col) if filename_col in r else None
                    if isinstance(fname_val, str) and fname_val.strip().lower() == 'nan':
                        fname_val = None
                    if (logic_val in (None, '') or str(logic_val).strip() == '') and (fname_val is None or (isinstance(fname_val, str) and fname_val.strip() == '')):
                        continue
                    doc_id = str(uuid.uuid4())
                    doc = {
                        "id": doc_id,
                        "document_name": '' if fname_val is None else str(fname_val),
                        "document_url": '',
                        "application_id": app_id,
                        "file_name": base,
                        "documents_order": order,
                        "logic_doc_id": '' if logic_val in (None, '') else str(int(logic_val)),
                    }
                    order += 1
                    doc_rows.append(doc)

            # If preview: just count and continue
            if preview:
                inserted_apps += len(app_rows)
                inserted_docs += len(doc_rows)
                offset += batch_size
                print(f"(preview) Processed offset {offset}: apps {inserted_apps}, docs {inserted_docs}")
                continue

            # Prepare CSV for apps
            app_fieldnames = ['id'] + copy_cols + ['created_at']
            app_buf = io.StringIO()
            writer = csv.DictWriter(app_buf, fieldnames=app_fieldnames, extrasaction='ignore')
            writer.writeheader()
            for a in app_rows:
                row = {k: a.get(k, '') for k in ['id'] + copy_cols}
                row['created_at'] = ''
                writer.writerow(row)
            app_buf.seek(0)

            # COPY apps
            copy_sql = f"COPY ca_applications ({','.join(app_fieldnames)}) FROM STDIN WITH (FORMAT csv, HEADER true)"
            try:
                cur.copy_expert(copy_sql, app_buf)
            except Exception:
                raw_conn.rollback()
                raise

            # Prepare CSV for docs
            doc_fieldnames = ['id', 'document_name', 'document_url', 'application_id', 'file_name', 'documents_order', 'logic_doc_id', 'created_at']
            doc_buf = io.StringIO()
            writer = csv.DictWriter(doc_buf, fieldnames=doc_fieldnames, extrasaction='ignore')
            writer.writeheader()
            for d in doc_rows:
                row = {
                    'id': d.get('id', ''),
                    'document_name': d.get('document_name', ''),
                    'document_url': d.get('document_url', ''),
                    'application_id': d.get('application_id', ''),
                    'file_name': d.get('file_name', ''),
                    'documents_order': d.get('documents_order', ''),
                    'logic_doc_id': d.get('logic_doc_id', ''),
                    'created_at': '',
                }
                writer.writerow(row)
            doc_buf.seek(0)

            copy_sql = f"COPY ca_documents ({','.join(doc_fieldnames)}) FROM STDIN WITH (FORMAT csv, HEADER true)"
            try:
                cur.copy_expert(copy_sql, doc_buf)
            except Exception:
                raw_conn.rollback()
                raise

            # Commit batch
            raw_conn.commit()
            inserted_apps += len(app_rows)
            inserted_docs += len(doc_rows)
            offset += batch_size
            print(f"Processed offset {offset}: apps {inserted_apps}, docs {inserted_docs}")

        print("Done. Inserted apps:", inserted_apps, "docs:", inserted_docs)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        raw_conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-table', default='applications')
    parser.add_argument('--batch-size', default=200, type=int)
    parser.add_argument('--preview', action='store_true')
    parser.add_argument('--fast-copy', action='store_true', help='Use COPY FROM STDIN per batch for faster imports')
    args = parser.parse_args()

    # Reuse the project's engine
    engine = get_engine()
    migrate(engine, source_table=args.source_table, batch_size=args.batch_size, preview=args.preview, fast_copy=args.fast_copy)
