-- =======================================================================
-- Backfill certificate_owner for existing certificates
-- =======================================================================

-- 1. Electrical Installations
-- Populate certificate_owner from application_electrical_installation.applicant_name
UPDATE public.certificates c
SET 
    certificate_owner = aei.applicant_name,
    updated_at = now()
FROM public.application_electrical_installation aei
WHERE c.application_id = aei.application_id
  AND c.sector = 'ELECTRICITY'
  AND (c.certificate_owner IS NULL OR c.certificate_owner = '');

-- 2. Other Sectors (Water Supply, Petroleum, Natural Gas, etc.)
-- Populate certificate_owner from application_sector_details
-- facility_name + po_box (if po_box is not null)
UPDATE public.certificates c
SET 
    certificate_owner = TRIM(
        COALESCE(asd.facility_name, '') || 
        CASE 
            WHEN asd.po_box IS NOT NULL AND asd.po_box <> '' THEN ', ' || asd.po_box 
            ELSE '' 
        END
    ),
    updated_at = now()
FROM public.application_sector_details asd
WHERE c.application_id = asd.application_id
  AND c.sector IN ('PETROLEUM', 'NATURAL_GAS', 'WATER_SUPPLY')
  AND (
      c.certificate_owner IS NULL 
      OR c.certificate_owner = ''
      OR c.certificate_owner = TRIM(
          COALESCE(asd.facility_name, '') || 
          CASE 
              WHEN asd.po_box IS NOT NULL AND asd.po_box <> '' THEN ' ' || asd.po_box 
              ELSE '' 
          END
      )
  );
