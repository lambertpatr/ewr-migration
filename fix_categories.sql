BEGIN;

UPDATE public.applications 
SET category_id = '14de314a-c7d9-42ed-a0b6-77886a9c2e89'::uuid 
WHERE application_number = 'NGPO/2023/0003';

UPDATE public.applications 
SET category_id = 'cf56a2b4-6af9-4122-b993-ee877c2c08c4'::uuid 
WHERE application_number = 'CNGPR/2026/0000003';

UPDATE public.applications 
SET category_id = 'd3c511a6-1886-4493-93d5-bc12af56f93a'::uuid 
WHERE application_number = 'CNGFS/2026/000001';

UPDATE public.applications 
SET category_id = 'a1668ef2-cd88-41b3-87c7-009622e4de57'::uuid 
WHERE application_number = 'AESOL/2025/0001';

COMMIT;
