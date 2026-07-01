BEGIN;

DROP MATERIALIZED VIEW IF EXISTS public.tanesco_api_mv CASCADE;

CREATE MATERIALIZED VIEW public.tanesco_api_mv AS
WITH ranked_certificates AS (
    SELECT COALESCE(asd.company_name, aei.applicant_name, asd.facility_name) AS customer_name,
           COALESCE(c.approval_no, a.approval_no)                            AS license_no,
           COALESCE(c.effective_date, c.approval_date, a.approval_date)      AS approved_date,
           c.expire_date                                                     AS expiry_date,
           COALESCE(asd.region, a.region)                                    AS region_name,
           COALESCE(cd.mobile_no, asd.mobile_no, aei.mobile_no)              AS phone_no,
           COALESCE(cd.email, asd.email, aei.email)                          AS email,
           aei.decided_class_name                                            AS class,
           z.name                                                            AS zone_name,
           COALESCE(asd.district, a.district)                                AS district_name,
           COALESCE(asd.ward, a.ward)                                        AS ward_name,
           c.created_at,
           c.npgis_status,
           row_number() OVER (PARTITION BY (COALESCE(c.approval_no, a.approval_no)) ORDER BY c.created_at DESC) AS rn
    FROM public.certificates c
             LEFT JOIN public.applications a ON a.id = c.application_id
             LEFT JOIN public.zones z ON z.id = a.zone_id
             LEFT JOIN public.application_sector_details asd ON asd.application_id = a.id
             LEFT JOIN public.application_electrical_installation aei ON aei.application_id = a.id
             LEFT JOIN public.contact_details cd ON cd.application_id = a.id
    WHERE (c.approval_no IS NOT NULL OR a.approval_no IS NOT NULL)
      AND c.sector::text = 'ELECTRICITY'::text
      AND c.certificate_type::text = 'License'::text
)
SELECT customer_name,
       license_no,
       approved_date,
       expiry_date,
       region_name,
       phone_no,
       email,
       class,
       zone_name,
       district_name,
       ward_name
FROM ranked_certificates
WHERE rn = 1;

CREATE UNIQUE INDEX idx_tanesco_api_mv_license_no ON public.tanesco_api_mv (license_no);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.tanesco_api_mv TO appuser;

COMMIT;
