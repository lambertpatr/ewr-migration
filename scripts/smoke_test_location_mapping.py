"""Quick smoke-test for region/district/ward id->name mapping.

This doesn't touch the DB. It just validates that the CSV-backed maps load
and that numeric/scientific-notation inputs map correctly.

Run:
  python -m scripts.smoke_test_location_mapping
"""

from __future__ import annotations

from app.services.application_migrations_service import (
    district_map_csv,
    region_map_csv,
    ward_map,
    _normalize_numeric_string,
)


def _lookup(m: dict[str, str], v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s in m:
        return m[s]
    nk = _normalize_numeric_string(s)
    if nk and nk in m:
        return m[nk]
    return None


def main() -> None:
    print("loaded:")
    print("  regions  :", len(region_map_csv))
    print("  districts:", len(district_map_csv))
    print("  wards    :", len(ward_map))

    # A couple of known ids (from the CSV headers shown)
    print("examples:")
    print("  region 1553779224267 ->", _lookup(region_map_csv, 1553779224267))
    print("  district 1554087241462 ->", _lookup(district_map_csv, 1554087241462))
    print("  ward 1554087241741 ->", _lookup(ward_map, 1554087241741))

    # Scientific notation simulation
    print("  ward 1.554087241741e+12 ->", _lookup(ward_map, "1.554087241741e+12"))


if __name__ == "__main__":
    main()
