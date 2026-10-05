-- =============================================================================
-- EWURA e-Services Compliance: Fast Analytical Reporting Tables & Refresh Routines
-- Target Database: eservice_compliance on 10.1.8.166
-- Role/Owner: appuser
-- Schedule: Every 2 minutes via crontab executing CALL public.call_refresh_functions();
-- =============================================================================

-- =============================================================================
-- 1. APPROVED ACTION PLAN REPORT
-- =============================================================================
DROP TABLE IF EXISTS public.approved_action_plan_rpt CASCADE;

CREATE TABLE public.approved_action_plan_rpt (
    activity_id            uuid PRIMARY KEY,
    financial_year         varchar,
    action_plan_title      varchar,
    responsible            varchar,
    objective_code         varchar,
    target_code            varchar,
    target_description     text,
    activity_description   text,
    approved_budget        numeric,
    q1 boolean,
    q2 boolean,
    q3 boolean,
    q4 boolean,
    deliverables           text,
    impacts                text,
    last_refreshed_at      timestamptz
)
WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS ix_aap_rpt_financial_year ON public.approved_action_plan_rpt (financial_year) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_aap_rpt_action_plan ON public.approved_action_plan_rpt (action_plan_title) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_aap_rpt_objective ON public.approved_action_plan_rpt (objective_code) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_aap_rpt_target ON public.approved_action_plan_rpt (target_code) WITH (fillfactor = 80);

ALTER TABLE public.approved_action_plan_rpt OWNER TO appuser;

CREATE OR REPLACE FUNCTION public.refresh_approved_action_plan_rpt()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
    v_last_refreshed_at timestamptz;
    v_rows_affected     integer;
BEGIN
    SELECT MAX(last_refreshed_at)
    INTO v_last_refreshed_at
    FROM public.approved_action_plan_rpt;

    INSERT INTO public.approved_action_plan_rpt (
        activity_id,
        financial_year,
        action_plan_title,
        responsible,
        objective_code,
        target_code,
        target_description,
        activity_description,
        approved_budget,
        q1, q2, q3, q4,
        deliverables,
        impacts,
        last_refreshed_at
    )
    SELECT
        act.id,
        fy.title,
        ap.title,
        ap.department_name,
        obj.code,
        tgt.code,
        tgt.description,
        act.description,
        act.approved_budget,
        BOOL_OR(aqp.quarter = 'Q1'),
        BOOL_OR(aqp.quarter = 'Q2'),
        BOOL_OR(aqp.quarter = 'Q3'),
        BOOL_OR(aqp.quarter = 'Q4'),
        STRING_AGG(DISTINCT ed.deliverable, '; '),
        STRING_AGG(DISTINCT ei.impact, '; '),
        now()
    FROM public.activities act
    LEFT JOIN public.action_plans ap ON ap.id = act.action_plan_id
    LEFT JOIN public.financial_years fy ON fy.id = ap.financial_year_id
    LEFT JOIN public.targets tgt ON tgt.id = act.target_id
    LEFT JOIN public.objectives obj ON obj.id = tgt.objective_id AND obj.financial_year_id = fy.id
    LEFT JOIN public.activity_quarter_plans aqp ON aqp.activity_id = act.id
    LEFT JOIN public.expected_deliverables ed ON ed.activity_id = act.id
    LEFT JOIN public.expected_impacts ei ON ei.activity_id = act.id
    WHERE act.updated_at >= COALESCE(v_last_refreshed_at, '-infinity'::timestamptz)
    GROUP BY
        act.id,
        fy.title,
        ap.title,
        ap.department_name,
        obj.code,
        tgt.code,
        tgt.description,
        act.description,
        act.approved_budget
    ON CONFLICT (activity_id) DO UPDATE
    SET
        financial_year        = EXCLUDED.financial_year,
        action_plan_title     = EXCLUDED.action_plan_title,
        responsible           = EXCLUDED.responsible,
        objective_code        = EXCLUDED.objective_code,
        target_code           = EXCLUDED.target_code,
        target_description    = EXCLUDED.target_description,
        activity_description  = EXCLUDED.activity_description,
        approved_budget       = EXCLUDED.approved_budget,
        q1                    = EXCLUDED.q1,
        q2                    = EXCLUDED.q2,
        q3                    = EXCLUDED.q3,
        q4                    = EXCLUDED.q4,
        deliverables          = EXCLUDED.deliverables,
        impacts               = EXCLUDED.impacts,
        last_refreshed_at     = EXCLUDED.last_refreshed_at;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'approved_action_plan_rpt refreshed successfully. Rows affected: %', v_rows_affected;
    RETURN v_rows_affected;
END;
$func$;

ALTER FUNCTION public.refresh_approved_action_plan_rpt() OWNER TO appuser;

-- =============================================================================
-- 2. ANNUAL COMPLIANCE PROGRESSIVE REPORT
-- =============================================================================
DROP TABLE IF EXISTS public.annual_compliance_progressive_rpt CASCADE;

CREATE TABLE public.annual_compliance_progressive_rpt (
    activity_id             uuid PRIMARY KEY,
    financial_year          varchar,
    action_plan_title       varchar,
    responsible             varchar,
    objective_code          varchar,
    target_code             varchar,
    target_description      text,
    activity_description    text,
    approved_budget         numeric,
    quarter1_targeted_value numeric,
    quarter2_targeted_value numeric,
    quarter3_targeted_value numeric,
    quarter4_targeted_value numeric,
    actual_perf_q1          numeric,
    actual_perf_q2          numeric,
    actual_perf_q3          numeric,
    actual_perf_q4          numeric,
    perf_against_q1         numeric,
    perf_against_q2         numeric,
    perf_against_q3         numeric,
    perf_against_q4         numeric,
    tangible_output         text,
    outcome                 text,
    last_refreshed_at       timestamptz
)
WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS ix_acpr_financial_year ON public.annual_compliance_progressive_rpt (financial_year) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_acpr_action_plan ON public.annual_compliance_progressive_rpt (action_plan_title) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_acpr_objective ON public.annual_compliance_progressive_rpt (objective_code) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_acpr_target ON public.annual_compliance_progressive_rpt (target_code) WITH (fillfactor = 80);

ALTER TABLE public.annual_compliance_progressive_rpt
SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

ALTER TABLE public.annual_compliance_progressive_rpt OWNER TO appuser;

CREATE OR REPLACE FUNCTION public.refresh_annual_compliance_progressive_rpt()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
    v_last_refreshed_at timestamptz;
    v_rows_affected     integer;
BEGIN
    SELECT MAX(last_refreshed_at)
    INTO v_last_refreshed_at
    FROM public.annual_compliance_progressive_rpt;

    INSERT INTO public.annual_compliance_progressive_rpt (
        activity_id,
        financial_year,
        action_plan_title,
        responsible,
        objective_code,
        target_code,
        target_description,
        activity_description,
        approved_budget,
        quarter1_targeted_value,
        quarter2_targeted_value,
        quarter3_targeted_value,
        quarter4_targeted_value,
        actual_perf_q1,
        actual_perf_q2,
        actual_perf_q3,
        actual_perf_q4,
        perf_against_q1,
        perf_against_q2,
        perf_against_q3,
        perf_against_q4,
        tangible_output,
        outcome,
        last_refreshed_at
    )
    SELECT
        act.id,
        fy.title,
        ap.title,
        ap.department_name,
        obj.code,
        tgt.code,
        tgt.description,
        act.description,
        act.approved_budget,
        MAX(aqp.indicator_value::numeric) FILTER (WHERE aqp.quarter = 'Q1'),
        MAX(aqp.indicator_value::numeric) FILTER (WHERE aqp.quarter = 'Q2'),
        MAX(aqp.indicator_value::numeric) FILTER (WHERE aqp.quarter = 'Q3'),
        MAX(aqp.indicator_value::numeric) FILTER (WHERE aqp.quarter = 'Q4'),
        MAX(aqpr.current_value::numeric) FILTER (WHERE aqp.quarter = 'Q1'),
        MAX(aqpr.current_value::numeric) FILTER (WHERE aqp.quarter = 'Q2'),
        MAX(aqpr.current_value::numeric) FILTER (WHERE aqp.quarter = 'Q3'),
        MAX(aqpr.current_value::numeric) FILTER (WHERE aqp.quarter = 'Q4'),
        MAX(aqpr.progress_rate::numeric) FILTER (WHERE aqp.quarter = 'Q1'),
        MAX(aqpr.progress_rate::numeric) FILTER (WHERE aqp.quarter = 'Q2'),
        MAX(aqpr.progress_rate::numeric) FILTER (WHERE aqp.quarter = 'Q3'),
        MAX(aqpr.progress_rate::numeric) FILTER (WHERE aqp.quarter = 'Q4'),
        STRING_AGG(DISTINCT ed.deliverable, '; '),
        STRING_AGG(DISTINCT ei.impact, '; '),
        now()
    FROM public.activities act
    LEFT JOIN public.action_plans ap ON ap.id = act.action_plan_id
    LEFT JOIN public.financial_years fy ON fy.id = ap.financial_year_id
    LEFT JOIN public.targets tgt ON tgt.id = act.target_id
    LEFT JOIN public.objectives obj ON obj.id = tgt.objective_id AND obj.financial_year_id = fy.id
    LEFT JOIN public.activity_quarter_plans aqp ON aqp.activity_id = act.id
    LEFT JOIN public.activity_quarter_progress aqpr ON aqpr.activity_quarter_plan_id = aqp.id
    LEFT JOIN public.expected_deliverables ed ON ed.activity_id = act.id
    LEFT JOIN public.expected_impacts ei ON ei.activity_id = act.id
    WHERE act.updated_at >= COALESCE(v_last_refreshed_at, '-infinity'::timestamptz)
    GROUP BY
        act.id,
        fy.title,
        ap.title,
        ap.department_name,
        obj.code,
        tgt.code,
        tgt.description,
        act.description,
        act.approved_budget
    ON CONFLICT (activity_id) DO UPDATE
    SET
        financial_year            = EXCLUDED.financial_year,
        action_plan_title         = EXCLUDED.action_plan_title,
        responsible               = EXCLUDED.responsible,
        objective_code            = EXCLUDED.objective_code,
        target_code               = EXCLUDED.target_code,
        target_description        = EXCLUDED.target_description,
        activity_description      = EXCLUDED.activity_description,
        approved_budget           = EXCLUDED.approved_budget,
        quarter1_targeted_value   = EXCLUDED.quarter1_targeted_value,
        quarter2_targeted_value   = EXCLUDED.quarter2_targeted_value,
        quarter3_targeted_value   = EXCLUDED.quarter3_targeted_value,
        quarter4_targeted_value   = EXCLUDED.quarter4_targeted_value,
        actual_perf_q1            = EXCLUDED.actual_perf_q1,
        actual_perf_q2            = EXCLUDED.actual_perf_q2,
        actual_perf_q3            = EXCLUDED.actual_perf_q3,
        actual_perf_q4            = EXCLUDED.actual_perf_q4,
        perf_against_q1           = EXCLUDED.perf_against_q1,
        perf_against_q2           = EXCLUDED.perf_against_q2,
        perf_against_q3           = EXCLUDED.perf_against_q3,
        perf_against_q4           = EXCLUDED.perf_against_q4,
        tangible_output           = EXCLUDED.tangible_output,
        outcome                   = EXCLUDED.outcome,
        last_refreshed_at         = EXCLUDED.last_refreshed_at;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'annual_compliance_progressive_rpt refreshed. Rows affected: %', v_rows_affected;
    RETURN v_rows_affected;
END;
$func$;

ALTER FUNCTION public.refresh_annual_compliance_progressive_rpt() OWNER TO appuser;

-- =============================================================================
-- 3. QUARTER COMPLIANCE PROGRESSIVE REPORT (RPT SUMMARY)
-- =============================================================================
DROP TABLE IF EXISTS public.quarter_compliance_progressive_rpt CASCADE;

CREATE TABLE public.quarter_compliance_progressive_rpt (
    activity_id                uuid NOT NULL,
    quarter                    varchar(2) NOT NULL,
    financial_year             varchar,
    responsible                varchar,
    target_description         text,
    activity_description       text,
    approved_budget            numeric,
    targeted_value             numeric,
    actual_performance         numeric,
    performance_against_target numeric,
    actual_expenses            numeric,
    tangible_output            text,
    outcome                    text,
    has_missing_target         boolean,
    has_missing_actual         boolean,
    is_over_budget             boolean,
    is_below_target            boolean,
    last_refreshed_at          timestamptz,
    CONSTRAINT pk_quarter_compliance_progressive_rpt PRIMARY KEY (activity_id, quarter)
)
WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS ix_qcpr_financial_year ON public.quarter_compliance_progressive_rpt (financial_year) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_qcpr_quarter ON public.quarter_compliance_progressive_rpt (quarter) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_qcpr_responsible ON public.quarter_compliance_progressive_rpt (responsible) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_qcpr_missing_target ON public.quarter_compliance_progressive_rpt (activity_id) WHERE has_missing_target = true;
CREATE INDEX IF NOT EXISTS ix_qcpr_missing_actual ON public.quarter_compliance_progressive_rpt (activity_id) WHERE has_missing_actual = true;
CREATE INDEX IF NOT EXISTS ix_qcpr_over_budget ON public.quarter_compliance_progressive_rpt (activity_id) WHERE is_over_budget = true;
CREATE INDEX IF NOT EXISTS ix_qcpr_below_target ON public.quarter_compliance_progressive_rpt (activity_id) WHERE is_below_target = true;

ALTER TABLE public.quarter_compliance_progressive_rpt OWNER TO appuser;

CREATE OR REPLACE FUNCTION public.refresh_quarter_compliance_progressive_rpt()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
    v_last_refreshed_at timestamptz;
    v_rows_affected     integer;
BEGIN
    SELECT MAX(last_refreshed_at)
    INTO v_last_refreshed_at
    FROM public.quarter_compliance_progressive_rpt;

    INSERT INTO public.quarter_compliance_progressive_rpt (
        activity_id,
        financial_year,
        quarter,
        responsible,
        target_description,
        activity_description,
        approved_budget,
        targeted_value,
        actual_performance,
        performance_against_target,
        actual_expenses,
        tangible_output,
        outcome,
        has_missing_target,
        has_missing_actual,
        is_over_budget,
        is_below_target,
        last_refreshed_at
    )
    SELECT
        act.id,
        fy.title,
        aqp.quarter,
        ap.department_name,
        tgt.description,
        act.description,
        act.approved_budget,
        aqp.indicator_value::numeric,
        aqpr.current_value::numeric,
        aqpr.progress_rate::numeric,
        act.expenditure,
        STRING_AGG(DISTINCT ed.deliverable, '; '),
        STRING_AGG(DISTINCT ei.impact, '; '),
        (aqp.indicator_value IS NULL),
        (aqp.indicator_value IS NOT NULL AND aqpr.current_value IS NULL),
        (act.expenditure IS NOT NULL AND act.approved_budget IS NOT NULL AND act.expenditure > act.approved_budget),
        (aqpr.progress_rate IS NOT NULL AND aqpr.progress_rate < 0),
        now()
    FROM public.activities act
    LEFT JOIN public.action_plans ap ON ap.id = act.action_plan_id
    LEFT JOIN public.financial_years fy ON fy.id = ap.financial_year_id
    LEFT JOIN public.targets tgt ON tgt.id = act.target_id
    LEFT JOIN public.objectives obj ON obj.id = tgt.objective_id AND obj.financial_year_id = fy.id
    INNER JOIN public.activity_quarter_plans aqp ON aqp.activity_id = act.id
    INNER JOIN public.activity_quarter_progress aqpr ON aqpr.activity_quarter_plan_id = aqp.id
    LEFT JOIN public.expected_deliverables ed ON ed.activity_id = act.id
    LEFT JOIN public.expected_impacts ei ON ei.activity_id = act.id
    WHERE act.updated_at >= COALESCE(v_last_refreshed_at, '-infinity'::timestamptz)
    GROUP BY
        act.id, fy.title, aqp.quarter, ap.department_name, tgt.description,
        act.description, act.approved_budget, act.expenditure,
        aqp.indicator_value, aqpr.current_value, aqpr.progress_rate
    ON CONFLICT (activity_id, quarter)
    DO UPDATE SET
        financial_year                    = EXCLUDED.financial_year,
        responsible                       = EXCLUDED.responsible,
        target_description                = EXCLUDED.target_description,
        activity_description              = EXCLUDED.activity_description,
        approved_budget                   = EXCLUDED.approved_budget,
        targeted_value                    = EXCLUDED.targeted_value,
        actual_performance                = EXCLUDED.actual_performance,
        performance_against_target        = EXCLUDED.performance_against_target,
        actual_expenses                   = EXCLUDED.actual_expenses,
        tangible_output                   = EXCLUDED.tangible_output,
        outcome                           = EXCLUDED.outcome,
        has_missing_target                = EXCLUDED.has_missing_target,
        has_missing_actual                = EXCLUDED.has_missing_actual,
        is_over_budget                    = EXCLUDED.is_over_budget,
        is_below_target                   = EXCLUDED.is_below_target,
        last_refreshed_at                 = EXCLUDED.last_refreshed_at;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'quarter_compliance_progressive_rpt refreshed. Rows affected: %', v_rows_affected;
    RETURN v_rows_affected;
END;
$func$;

ALTER FUNCTION public.refresh_quarter_compliance_progressive_rpt() OWNER TO appuser;

-- =============================================================================
-- 4. QUARTER COMPLIANCE PROGRESSIVE REPORTS (MADUKA GRAIN)
-- =============================================================================
ALTER TABLE public.quarter_compliance_progressive_reports
    ALTER COLUMN id SET DEFAULT gen_random_uuid(),
    ALTER COLUMN activity_description TYPE text,
    ALTER COLUMN target_description TYPE text,
    ALTER COLUMN tungible_output TYPE text,
    ALTER COLUMN outcome TYPE text;

ALTER TABLE public.quarter_compliance_progressive_reports
    ADD COLUMN IF NOT EXISTS activity_quarter_plan_id uuid,
    ADD COLUMN IF NOT EXISTS action_plan_id uuid;

CREATE UNIQUE INDEX IF NOT EXISTS uk_qcpr_aqp ON public.quarter_compliance_progressive_reports (activity_quarter_plan_id);
CREATE INDEX IF NOT EXISTS idx_qcpr_last_refresh ON public.quarter_compliance_progressive_reports (last_refreshed_at);
CREATE INDEX IF NOT EXISTS idx_activities_updated_at ON public.activities (updated_at);
CREATE INDEX IF NOT EXISTS idx_aqp_activity ON public.activity_quarter_plans (activity_id);
CREATE INDEX IF NOT EXISTS idx_aqp_updated_at ON public.activity_quarter_plans (updated_at);
CREATE INDEX IF NOT EXISTS idx_aqpr_latest ON public.activity_quarter_progress (activity_quarter_plan_id, reporting_date DESC, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_ed_activity ON public.expected_deliverables (activity_id);
CREATE INDEX IF NOT EXISTS idx_ei_activity ON public.expected_impacts (activity_id);

CREATE INDEX IF NOT EXISTS ix_qcpr_financial_year ON public.quarter_compliance_progressive_reports (financial_year) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_qcpr_quarter ON public.quarter_compliance_progressive_reports (quarter) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_qcpr_responsible ON public.quarter_compliance_progressive_reports (responsible_organizational_level) WITH (fillfactor = 80);
CREATE INDEX IF NOT EXISTS ix_qcpr_missing_target ON public.quarter_compliance_progressive_reports (activity_id) WHERE has_missing_target = true;
CREATE INDEX IF NOT EXISTS ix_qcpr_missing_actual ON public.quarter_compliance_progressive_reports (activity_id) WHERE has_missing_actual = true;
CREATE INDEX IF NOT EXISTS ix_qcpr_over_budget ON public.quarter_compliance_progressive_reports (activity_id) WHERE is_over_budget = true;
CREATE INDEX IF NOT EXISTS ix_qcpr_below_target ON public.quarter_compliance_progressive_reports (activity_id) WHERE is_below_target = true;

ALTER TABLE public.quarter_compliance_progressive_reports SET (fillfactor = 80);
ALTER TABLE public.quarter_compliance_progressive_reports OWNER TO appuser;

CREATE OR REPLACE FUNCTION public.refresh_quarter_compliance_progressive_report()
RETURNS integer
LANGUAGE plpgsql
AS $func$
DECLARE
    v_last_refreshed_at timestamptz;
    v_rows_affected     integer;
BEGIN
    SELECT COALESCE(MAX(last_refreshed_at), '-infinity'::timestamptz)
    INTO v_last_refreshed_at
    FROM public.quarter_compliance_progressive_reports;

    INSERT INTO public.quarter_compliance_progressive_reports (
        activity_quarter_plan_id,
        action_plan_id,
        activity_id,
        activity_code,
        financial_year,
        quarter,
        responsible_organizational_level,
        target_description,
        activity_description,
        budget_estimates,
        targeted_value,
        actual_perfomance,
        perfomance_against_target,
        actual_expenses,
        tungible_output,
        outcome,
        has_missing_target,
        has_missing_actual,
        is_over_budget,
        is_below_target,
        last_refreshed_at,
        created_at
    )
    SELECT
        aqp.id,
        act.action_plan_id,
        act.id,
        act.code,
        fy.title,
        aqp.quarter,
        ap.department_name,
        tgt.description,
        act.description,
        act.approved_budget,
        aqp.indicator_value::numeric,
        aqpr.current_value::numeric,
        aqpr.progress_rate::numeric,
        act.expenditure,
        STRING_AGG(DISTINCT ed.deliverable, '; '),
        STRING_AGG(DISTINCT ei.impact, '; '),
        (aqp.indicator_value IS NULL),
        (aqp.indicator_value IS NOT NULL AND aqpr.current_value IS NULL),
        (act.expenditure IS NOT NULL AND act.approved_budget IS NOT NULL AND act.expenditure > act.approved_budget),
        (aqpr.progress_rate IS NOT NULL AND aqpr.progress_rate < 0),
        now(),
        now()
    FROM public.activities act
    LEFT JOIN public.action_plans ap ON ap.id = act.action_plan_id
    LEFT JOIN public.financial_years fy ON fy.id = ap.financial_year_id
    LEFT JOIN public.targets tgt ON tgt.id = act.target_id
    INNER JOIN public.activity_quarter_plans aqp ON aqp.activity_id = act.id
    LEFT JOIN LATERAL (
        SELECT aqpr.current_value, aqpr.progress_rate, aqpr.updated_at
        FROM public.activity_quarter_progress aqpr
        WHERE aqpr.activity_quarter_plan_id = aqp.id AND aqpr.deleted_at IS NULL
        ORDER BY aqpr.reporting_date DESC NULLS LAST, aqpr.updated_at DESC NULLS LAST
        LIMIT 1
    ) aqpr ON TRUE
    LEFT JOIN public.expected_deliverables ed ON ed.activity_id = act.id
    LEFT JOIN public.expected_impacts ei ON ei.activity_id = act.id
    WHERE GREATEST(
        COALESCE(act.updated_at, '-infinity'::timestamp),
        COALESCE(aqp.updated_at, '-infinity'::timestamp),
        COALESCE(aqpr.updated_at, '-infinity'::timestamp)
    ) >= v_last_refreshed_at
    GROUP BY
        aqp.id, act.action_plan_id, act.id, act.code, fy.title, aqp.quarter,
        ap.department_name, tgt.description, act.description, act.approved_budget,
        act.expenditure, aqp.indicator_value, aqpr.current_value, aqpr.progress_rate,
        act.updated_at, aqp.updated_at, aqpr.updated_at
    ON CONFLICT (activity_quarter_plan_id)
    DO UPDATE SET
        action_plan_id                   = EXCLUDED.action_plan_id,
        activity_code                    = EXCLUDED.activity_code,
        financial_year                   = EXCLUDED.financial_year,
        quarter                          = EXCLUDED.quarter,
        responsible_organizational_level = EXCLUDED.responsible_organizational_level,
        target_description               = EXCLUDED.target_description,
        activity_description             = EXCLUDED.activity_description,
        budget_estimates                 = EXCLUDED.budget_estimates,
        targeted_value                   = EXCLUDED.targeted_value,
        actual_perfomance                = EXCLUDED.actual_perfomance,
        perfomance_against_target        = EXCLUDED.perfomance_against_target,
        actual_expenses                  = EXCLUDED.actual_expenses,
        tungible_output                  = EXCLUDED.tungible_output,
        outcome                          = EXCLUDED.outcome,
        has_missing_target               = EXCLUDED.has_missing_target,
        has_missing_actual               = EXCLUDED.has_missing_actual,
        is_over_budget                   = EXCLUDED.is_over_budget,
        is_below_target                  = EXCLUDED.is_below_target,
        last_refreshed_at                = now();

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE 'quarter_compliance_progressive_reports refreshed. Rows affected: %', v_rows_affected;
    RETURN v_rows_affected;
END;
$func$;

ALTER FUNCTION public.refresh_quarter_compliance_progressive_report() OWNER TO appuser;

-- =============================================================================
-- 5. MASTER REFRESH PROCEDURE (DYNAMIC RUNNER)
-- =============================================================================
CREATE OR REPLACE PROCEDURE public.call_refresh_functions()
LANGUAGE plpgsql
AS $procedure$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT 
            n.nspname,
            p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname ILIKE '%refresh%'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND p.prokind = 'f'
          AND p.pronargs = 0
    LOOP
        BEGIN
            RAISE NOTICE 'Running %.%()', r.nspname, r.proname;
            EXECUTE format('SELECT %I.%I()', r.nspname, r.proname);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'FAILED %.%(): %', r.nspname, r.proname, SQLERRM;
        END;
    END LOOP;
END;
$procedure$;

ALTER PROCEDURE public.call_refresh_functions() OWNER TO appuser;
