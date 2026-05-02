"""
OMOP table metadata and runtime tunables for the merge feature.

OMOP_TABLES
-----------
fk_remaps: dict mapping column_name -> id_map name.
  Columns listed here will have their value looked up in the corresponding
  IDRemapper map and replaced with the target ID before insert.
  If the source ID is not found in the map the column is set to NULL.

insert_order determines processing sequence — lower numbers run first so
that referenced tables are populated before referencing ones.
  1  care_site   (referenced by provider, person, visit_occurrence)
  2  provider    (referenced by person, visit_occurrence, clinical tables)
  3  person
  4  death
  5  visit_occurrence
  6  visit_detail
  7  clinical tables
  8  derived tables
"""

OMOP_TABLES = {
    # ---- Reference / administrative (no person FK) -------------------------
    "care_site": {
        "label": "care_site", "domain": "admin",
        "description": "Care sites / facilities",
        "person_fk": None, "self_pk": "care_site_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["care_site_source_value"], "insert_order": 1,
    },
    "provider": {
        "label": "provider", "domain": "admin",
        "description": "Clinicians and providers",
        "person_fk": None, "self_pk": "provider_id", "visit_fk": None,
        "fk_remaps": {
            "care_site_id": "care_site",
        },
        "dedup_cols": ["provider_source_value"], "insert_order": 2,
    },
    # ---- Core --------------------------------------------------------------
    "person": {
        "label": "person", "domain": "core",
        "description": "Demographics, DOB, gender, race",
        "person_fk": "person_id", "self_pk": "person_id", "visit_fk": None,
        "fk_remaps": {
            "provider_id":  "provider",
            "care_site_id": "care_site",
        },
        "dedup_cols": ["person_source_value"], "insert_order": 3,
    },
    "death": {
        "label": "death", "domain": "core",
        "description": "Cause of death records",
        "person_fk": "person_id", "self_pk": None, "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id"], "insert_order": 4,
    },
    # ---- Visit -------------------------------------------------------------
    "visit_occurrence": {
        "label": "visit_occurrence", "domain": "visit",
        "description": "Inpatient / outpatient visits",
        "person_fk": "person_id", "self_pk": "visit_occurrence_id", "visit_fk": None,
        "fk_remaps": {
            "provider_id":  "provider",
            "care_site_id": "care_site",
        },
        "dedup_cols": ["person_id", "visit_start_date", "visit_concept_id"], "insert_order": 5,
    },
    "visit_detail": {
        "label": "visit_detail", "domain": "visit",
        "description": "Sub-visit encounter detail",
        "person_fk": "person_id", "self_pk": "visit_detail_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id":      "visit_occurrence",
            "parent_visit_detail_id":   "visit_detail",   # self-referencing
            "provider_id":              "provider",
            "care_site_id":             "care_site",
        },
        "dedup_cols": ["person_id", "visit_detail_start_date", "visit_detail_concept_id"], "insert_order": 6,
    },
    # ---- Clinical ----------------------------------------------------------
    "condition_occurrence": {
        "label": "condition_occurrence", "domain": "clinical",
        "description": "Diagnoses and conditions",
        "person_fk": "person_id", "self_pk": "condition_occurrence_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "condition_concept_id", "condition_start_date"], "insert_order": 7,
    },
    "drug_exposure": {
        "label": "drug_exposure", "domain": "clinical",
        "description": "Medications and prescriptions",
        "person_fk": "person_id", "self_pk": "drug_exposure_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "drug_concept_id", "drug_exposure_start_date"], "insert_order": 7,
    },
    "measurement": {
        "label": "measurement", "domain": "clinical",
        "description": "Labs, vitals, test results",
        "person_fk": "person_id", "self_pk": "measurement_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "measurement_concept_id", "measurement_date"], "insert_order": 7,
    },
    "observation": {
        "label": "observation", "domain": "clinical",
        "description": "Clinical observations",
        "person_fk": "person_id", "self_pk": "observation_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "observation_concept_id", "observation_date"], "insert_order": 7,
    },
    "procedure_occurrence": {
        "label": "procedure_occurrence", "domain": "clinical",
        "description": "Surgeries and procedures",
        "person_fk": "person_id", "self_pk": "procedure_occurrence_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "procedure_concept_id", "procedure_date"], "insert_order": 7,
    },
    "device_exposure": {
        "label": "device_exposure", "domain": "clinical",
        "description": "Medical devices",
        "person_fk": "person_id", "self_pk": "device_exposure_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "device_concept_id", "device_exposure_start_date"], "insert_order": 7,
    },
    "specimen": {
        "label": "specimen", "domain": "clinical",
        "description": "Biological specimens",
        "person_fk": "person_id", "self_pk": "specimen_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "specimen_concept_id", "specimen_date"], "insert_order": 7,
    },
    "note": {
        "label": "note", "domain": "clinical",
        "description": "Free-text clinical notes",
        "person_fk": "person_id", "self_pk": "note_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "note_date", "note_type_concept_id"], "insert_order": 7,
    },
    # ---- Derived -----------------------------------------------------------
    "observation_period": {
        "label": "observation_period", "domain": "derived",
        "description": "Observation period windows",
        "person_fk": "person_id", "self_pk": "observation_period_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "observation_period_start_date", "observation_period_end_date"], "insert_order": 8,
    },
    "condition_era": {
        "label": "condition_era", "domain": "derived",
        "description": "Derived condition eras",
        "person_fk": "person_id", "self_pk": "condition_era_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "condition_concept_id", "condition_era_start_date"], "insert_order": 8,
    },
    "drug_era": {
        "label": "drug_era", "domain": "derived",
        "description": "Derived drug eras",
        "person_fk": "person_id", "self_pk": "drug_era_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "drug_concept_id", "drug_era_start_date"], "insert_order": 8,
    },
    "dose_era": {
        "label": "dose_era", "domain": "derived",
        "description": "Derived dose eras",
        "person_fk": "person_id", "self_pk": "dose_era_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "drug_concept_id", "dose_era_start_date"], "insert_order": 8,
    },
}

# Tables that have no person_fk (admin/reference tables).
ADMIN_TABLES = {name for name, m in OMOP_TABLES.items() if m["person_fk"] is None}

# ---------------------------------------------------------------------------
# Streaming / batching tunables
# ---------------------------------------------------------------------------
# These control the trade-off between memory usage and number of round-trips
# to PostgreSQL. They are deliberately conservative so the tool comfortably
# fits in a few hundred MB of RAM even on huge OMOP databases. Raise them
# if you have lots of free memory and want more throughput.

FETCH_BATCH         = 5_000   # source rows pulled per cursor round-trip
INSERT_BATCH        = 1_000   # rows accumulated before executemany flush
DEDUP_FETCH_BATCH   = 10_000  # target dedup rows pulled per cursor round-trip
MAPPING_LOG_CAP     = 50_000  # row-level mapping kept in RAM; beyond this we
                              # store only a per-table summary
PATIENT_PAYLOAD_CAP = 10_000  # patients-with-new-data sent inline in
                              # scan_complete; UI shows truncated + a flag
PERSON_AUDIT_CAP    = 500_000 # person-identity audit rows shipped in summary
                              # for CSV export. Each row is small (~80 bytes
                              # serialized) so 500K ≈ 40MB JSON. Plenty for
                              # most OMOP sites; bump if you have more
                              # patients and your gateway can handle it.

# JavaScript Number is IEEE 754 double precision: only ints with absolute
# value < 2^53 round-trip through JSON.parse without precision loss.
JS_SAFE_INT_MAX =  (1 << 53) - 1   # 9_007_199_254_740_991
JS_SAFE_INT_MIN = -(1 << 53) + 1   # -9_007_199_254_740_991
