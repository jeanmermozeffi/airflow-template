-- ============================================================================
-- Data Quality Checks — Template
-- ============================================================================
-- Chaque bloc retourne un seul entier.
--   min_count → valeur doit être >= threshold  (table non vide, données fraîches)
--   max_count → valeur doit être <= threshold  (zéro violation, zéro orphelin)
--
-- Format : -- @nom_check  (marqueur de début de bloc)
-- Le DAG data_quality_dag.py parse ce fichier dynamiquement.
-- ============================================================================

-- ── DIMENSIONS ───────────────────────────────────────────────────────────────

-- @dim_record_count
SELECT COUNT(*)
FROM dwh.dim_record
WHERE record_id > 0;

-- @dim_record_no_null_key
-- Dimensions sans clé naturelle
SELECT COUNT(*)
FROM dwh.dim_record
WHERE external_id IS NULL OR TRIM(external_id::text) = '';

-- @dim_date_coverage
-- Vérifie que dim_date couvre l'année en cours
SELECT COUNT(*)
FROM dwh.dim_date
WHERE EXTRACT(YEAR FROM date_actual) = EXTRACT(YEAR FROM CURRENT_DATE);

-- ── FAITS ─────────────────────────────────────────────────────────────────────

-- @fact_events_count
SELECT COUNT(*)
FROM dwh.fact_events;

-- @fact_events_orphan_records
-- Événements avec record_id non présent dans dim_record
SELECT COUNT(*)
FROM dwh.fact_events fe
WHERE fe.record_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dwh.dim_record dr
      WHERE dr.record_id = fe.record_id
  );

-- @fact_events_freshness
-- Au moins un événement chargé dans les dernières 48h
SELECT COUNT(*)
FROM dwh.fact_events
WHERE _loaded_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours';

-- @fact_events_negative_amounts
-- Montants négatifs non autorisés
SELECT COUNT(*)
FROM dwh.fact_events
WHERE amount < 0;

-- @fact_events_future_dates
-- Événements avec une date > aujourd'hui
SELECT COUNT(*)
FROM dwh.fact_events
WHERE event_date > CURRENT_DATE;

-- ── MARTS ──────────────────────────────────────────────────────────────────────

-- @mart_summary_count
SELECT COUNT(*)
FROM mart.mart_summary;

-- ============================================================================
-- TEMPLATE : pour ajouter un nouveau check
-- ============================================================================
-- 1. Copier ce bloc et remplacer new_check_name + le SQL
-- 2. Ajouter l'entrée correspondante dans checks.yaml
--
-- -- @new_check_name
-- SELECT COUNT(*)
-- FROM schema.table
-- WHERE condition;
-- ============================================================================
