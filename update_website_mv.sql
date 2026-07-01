BEGIN;

DROP MATERIALIZED VIEW IF EXISTS public.website_api_mv CASCADE;

CREATE MATERIALIZED VIEW public.website_api_mv AS
 WITH ranked_certificates AS (
         SELECT COALESCE(c.application_number, a.application_number) AS application_ref_no,
            COALESCE(c.approval_no, a.approval_no) AS license_no,
                CASE
                    WHEN approved_cat.name IS NOT NULL THEN 'ELECTRICAL INSTALLATION'::character varying
                    ELSE cat.name
                END AS license_type,
            c.sector,
            COALESCE(asd.company_name, aei.applicant_name, asd.facility_name) AS company_name,
            COALESCE(approved_cat.name, cat.name) AS class,
            z.name AS zone_name,
            COALESCE(asd.region, a.region) AS region_name,
            COALESCE(asd.district, a.district) AS district_name,
            COALESCE(c.effective_date, c.approval_date, a.approval_date) AS approved_date,
            COALESCE(c.expire_date, a.expire_date) AS expire_date,
            asd.tin AS tin_no,
            COALESCE(asd.mobile_no, aei.mobile_no) AS phone_no,
            COALESCE(asd.email, aei.email) AS email,
            c.created_at,
            c.npgis_status,
            row_number() OVER (PARTITION BY (COALESCE(c.approval_no, a.approval_no)) ORDER BY c.created_at DESC) AS rn
           FROM certificates c
             LEFT JOIN applications a ON a.id = c.application_id
             LEFT JOIN categories cat ON cat.id = a.category_id
             LEFT JOIN zones z ON z.id = a.zone_id
             LEFT JOIN application_sector_details asd ON asd.application_id = a.id
             LEFT JOIN application_electrical_installation aei ON aei.application_id = a.id
             LEFT JOIN categories approved_cat ON approved_cat.id = aei.approved_class_id
          WHERE (c.approval_no IS NOT NULL OR a.approval_no IS NOT NULL) AND c.certificate_type::text = 'License'::text AND COALESCE(c.expire_date, a.expire_date) >= CURRENT_DATE
        )
 SELECT application_ref_no,
    license_no,
    license_type,
    sector,
    company_name,
    class,
    zone_name,
    region_name,
    district_name,
    approved_date,
    expire_date,
    tin_no,
    phone_no,
    email
   FROM ranked_certificates
  WHERE rn = 1;

CREATE UNIQUE INDEX idx_website_api_mv_license_no ON public.website_api_mv USING btree (license_no);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.website_api_mv TO appuser;

COMMIT;
