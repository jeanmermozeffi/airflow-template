-- ============================================================================
-- Data Cleaning Operations — Template
-- ============================================================================
-- Nettoyage des tables staging en deux phases :
--   Phase 1 — Technique : trim, NULL → défaut, normalisation mécanique
--   Phase 2 — Métier    : déduplication, consolidation, règles projet
--
-- Format : -- @nom_operation  (marqueur de début de bloc)
-- Le DAG data_cleaning_dag.py parse ce fichier dynamiquement.
-- Chaque bloc est isolé et transactionnel.
-- ============================================================================

-- ── SETUP ────────────────────────────────────────────────────────────────────

-- @setup_extensions
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ============================================================================
-- PHASE 1 — Nettoyage technique
-- ============================================================================

-- ── raw.stg_records (Phase 1) ─────────────────────────────────────────────

-- @stg_records_trim_text_fields
-- Supprimer les espaces avant/après dans tous les champs texte
UPDATE raw.stg_records
SET
    label       = TRIM(label),
    description = TRIM(description)
WHERE label       IS DISTINCT FROM TRIM(COALESCE(label, ''))
   OR description IS DISTINCT FROM TRIM(COALESCE(description, ''));

-- @stg_records_fill_null_integers
-- Remplacer les valeurs entières NULL par 0
UPDATE raw.stg_records
SET
    quantity = COALESCE(quantity, 0),
    amount   = COALESCE(amount, 0)
WHERE quantity IS NULL
   OR amount IS NULL;

-- @stg_records_normalize_status
-- Normaliser le champ status : NULL → 'unknown'
UPDATE raw.stg_records
SET status = 'unknown'
WHERE status IS NULL OR TRIM(status) = '';

-- ============================================================================
-- PHASE 2 — Nettoyage métier
-- ============================================================================

-- ── raw.stg_records (Phase 2) ─────────────────────────────────────────────

-- @stg_records_dedupe
-- Supprimer les doublons exacts sur (external_id), garder l'ID minimum
DELETE FROM raw.stg_records
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY id) AS rn
        FROM raw.stg_records
    ) ranked
    WHERE rn > 1
);

-- @stg_records_fix_case
-- Appliquer Title Case sur le champ label
UPDATE raw.stg_records
SET label = INITCAP(LOWER(TRIM(label)))
WHERE label IS NOT NULL AND label != '';

-- ============================================================================
-- TEMPLATE : pour ajouter une nouvelle opération
-- ============================================================================
-- 1. Copier ce bloc et remplacer new_operation_name + le SQL
-- 2. Ajouter l'entrée correspondante dans operations.yaml
--
-- -- @new_operation_name
-- UPDATE raw.stg_your_table
-- SET column = new_value
-- WHERE condition;
-- ============================================================================
