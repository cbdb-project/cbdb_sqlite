#!/usr/bin/env bash

# Recreate CBDB convenience views inside a SQLite database.
# Usage: scripts/create_views.sh path/to/database.db

set -euo pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 path/to/database.db" >&2
    exit 1
fi

DB_PATH=$1

if [[ ! -f "$DB_PATH" ]]; then
    echo "Error: database file '$DB_PATH' does not exist." >&2
    exit 1
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "Error: sqlite3 is required but not installed or not on PATH." >&2
    exit 1
fi

# View_AltnameData: joins alternate names with their type code and source text
# metadata.
echo "Creating view View_AltnameData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_AltnameData;
CREATE VIEW View_AltnameData AS
SELECT
    a.c_personid,
    a.c_alt_name,
    a.c_alt_name_chn,
    a.c_alt_name_type_code,
    codes.c_name_type_desc,
    codes.c_name_type_desc_chn,
    a.c_sequence,
    a.c_source,
    texts.c_title,
    texts.c_title_chn,
    a.c_pages,
    a.c_notes
FROM ALTNAME_DATA AS a
INNER JOIN ALTNAME_CODES AS codes
    ON codes.c_name_type_code = a.c_alt_name_type_code
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = a.c_source;
SQL
echo "Finished view View_AltnameData."

# View_Association: fan-out join on ASSOC_DATA to kinship, institution, genre,
# occasion, topic, address, and textual sources.
echo "Creating view View_Association..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_Association;
CREATE VIEW View_Association AS
SELECT
    a.c_personid,
    a.c_assoc_id AS c_node_id,
    assoc_person.c_name AS c_node_name,
    assoc_person.c_name_chn AS c_node_chn,
    a.c_assoc_code AS c_link_code,
    assoc_codes.c_assoc_desc AS c_link_desc,
    assoc_codes.c_assoc_desc_chn AS c_link_chn,
    a.c_kin_code,
    kin_codes.c_kinrel_chn,
    kin_codes.c_kinrel,
    kin_person.c_name AS c_kin_name,
    kin_person.c_name_chn AS c_kin_chn,
    a.c_assoc_kin_id,
    assoc_kin_codes.c_kinrel_chn AS c_assoc_kinrel_chn,
    assoc_kin_codes.c_kinrel AS c_assoc_kinrel,
    assoc_kin_person.c_name AS c_assoc_kin_name,
    assoc_kin_person.c_name_chn AS c_assoc_kin_chn,
    lit_codes.c_lit_genre_desc,
    lit_codes.c_lit_genre_desc_chn,
    occasion_codes.c_occasion_desc,
    occasion_codes.c_occasion_desc_chn,
    topic_codes.c_topic_desc,
    topic_codes.c_topic_desc_chn,
    inst_names.c_inst_name_py,
    inst_names.c_inst_name_hz,
    a.c_text_title,
    a.c_assoc_claimer_id,
    assoc_claimer.c_name AS C_assoc_claimer_name,
    assoc_claimer.c_name_chn AS c_assoc_claimer_chn,
    text_codes.c_title AS c_source_title,
    text_codes.c_title_chn AS c_source_chn,
    a.c_notes,
    a.c_pages,
    a.c_sequence,
    a.c_assoc_count AS c_link_count,
    addr_codes.c_name AS c_assoc_addr_name,
    addr_codes.c_name_chn AS c_assoc_addr_chn,
    a.c_assoc_first_year,
    range_codes.c_range,
    range_codes.c_range_chn,
    a.c_assoc_fy_intercalary,
    a.c_assoc_fy_month,
    a.c_assoc_fy_day,
    nh.c_nianhao_chn AS c_assoc_fy_nh_chn,
    nh.c_nianhao_pin AS c_assoc_fy_nh_py,
    a.c_assoc_fy_nh_year,
    gz.c_ganzhi_chn,
    gz.c_ganzhi_py
FROM ASSOC_DATA AS a
INNER JOIN ASSOC_CODES AS assoc_codes
    ON assoc_codes.c_assoc_code = a.c_assoc_code
INNER JOIN BIOG_MAIN AS assoc_person
    ON assoc_person.c_personid = a.c_assoc_id
LEFT JOIN KINSHIP_CODES AS kin_codes
    ON kin_codes.c_kincode = a.c_kin_code
LEFT JOIN BIOG_MAIN AS kin_person
    ON kin_person.c_personid = a.c_kin_id
LEFT JOIN KINSHIP_CODES AS assoc_kin_codes
    ON assoc_kin_codes.c_kincode = a.c_assoc_kin_code
LEFT JOIN BIOG_MAIN AS assoc_kin_person
    ON assoc_kin_person.c_personid = a.c_assoc_kin_id
LEFT JOIN BIOG_MAIN AS assoc_claimer
    ON assoc_claimer.c_personid = a.c_assoc_claimer_id
LEFT JOIN LITERARYGENRE_CODES AS lit_codes
    ON lit_codes.c_lit_genre_code = a.c_litgenre_code
LEFT JOIN OCCASION_CODES AS occasion_codes
    ON occasion_codes.c_occasion_code = a.c_occasion_code
LEFT JOIN SCHOLARLYTOPIC_CODES AS topic_codes
    ON topic_codes.c_topic_code = a.c_topic_code
LEFT JOIN ADDR_CODES AS addr_codes
    ON addr_codes.c_addr_id = a.c_addr_id
LEFT JOIN YEAR_RANGE_CODES AS range_codes
    ON range_codes.c_range_code = a.c_assoc_fy_range
LEFT JOIN NIAN_HAO AS nh
    ON nh.c_nianhao_id = a.c_assoc_fy_nh_code
LEFT JOIN GANZHI_CODES AS gz
    ON gz.c_ganzhi_code = a.c_assoc_fy_day_gz
LEFT JOIN TEXT_CODES AS text_codes
    ON text_codes.c_textid = a.c_source
LEFT JOIN SOCIAL_INSTITUTION_NAME_CODES AS inst_names
    ON inst_names.c_inst_name_code = a.c_inst_name_code;
SQL
echo "Finished view View_Association."

# View_BiogAddrData: enriches BIOG_ADDR_DATA with address labels, reign data,
# ganzhi, and text citations.
echo "Creating view View_BiogAddrData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogAddrData;
CREATE VIEW View_BiogAddrData AS
SELECT
    b.c_personid,
    b.c_addr_id,
    addr.c_name AS c_addr_name,
    addr.c_name_chn AS c_addr_chn,
    addr_codes.c_addr_desc,
    addr_codes.c_addr_desc_chn,
    b.c_firstyear,
    b.c_lastyear,
    b.c_source,
    texts.c_title_chn AS c_source_chn,
    texts.c_title AS c_source_title,
    b.c_pages,
    b.c_notes,
    fy_nh.c_nianhao_chn AS c_fy_nh_chn,
    fy_nh.c_nianhao_pin AS c_fy_nh_py,
    b.c_fy_nh_year,
    b.c_fy_month,
    b.c_fy_day,
    fy_gz.c_ganzhi_chn AS c_fy_day_gz_chn,
    fy_gz.c_ganzhi_py AS c_fy_day_gz_py,
    b.c_fy_intercalary,
    fy_range.c_range AS c_fy_range_desc,
    fy_range.c_range_chn AS c_fy_range_chn,
    ly_nh.c_nianhao_chn AS c_ly_nh_chn,
    ly_nh.c_nianhao_pin AS c_ly_nh_py,
    b.c_ly_nh_year,
    b.c_ly_intercalary,
    b.c_ly_month,
    b.c_ly_day,
    ly_gz.c_ganzhi_chn AS c_ly_day_gz_chn,
    ly_gz.c_ganzhi_py AS c_ly_day_gz_py,
    ly_range.c_range AS c_ly_range_desc,
    ly_range.c_range_chn AS c_ly_range_chn,
    b.c_natal,
    b.c_fy_nh_code,
    b.c_ly_nh_code,
    b.c_sequence
FROM BIOG_ADDR_DATA AS b
INNER JOIN BIOG_ADDR_CODES AS addr_codes
    ON addr_codes.c_addr_type = b.c_addr_type
INNER JOIN ADDR_CODES AS addr
    ON addr.c_addr_id = b.c_addr_id
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = b.c_source
LEFT JOIN NIAN_HAO AS fy_nh
    ON fy_nh.c_nianhao_id = b.c_fy_nh_code
LEFT JOIN NIAN_HAO AS ly_nh
    ON ly_nh.c_nianhao_id = b.c_ly_nh_code
LEFT JOIN YEAR_RANGE_CODES AS fy_range
    ON fy_range.c_range_code = b.c_fy_range
LEFT JOIN YEAR_RANGE_CODES AS ly_range
    ON ly_range.c_range_code = b.c_ly_range
LEFT JOIN GANZHI_CODES AS fy_gz
    ON fy_gz.c_ganzhi_code = b.c_fy_day_gz
LEFT JOIN GANZHI_CODES AS ly_gz
    ON ly_gz.c_ganzhi_code = b.c_ly_day_gz;
SQL
echo "Finished view View_BiogAddrData."

# View_BiogInstData: expands BIOG_INST_DATA with institution names, role codes,
# reign info, and coordinates.
echo "Creating view View_BiogInstAddrData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogInstAddrData;
CREATE VIEW View_BiogInstAddrData AS
SELECT
    View_BiogInstData.*,
    SOCIAL_INSTITUTION_ADDR_TYPES.c_inst_addr_type_desc,
    SOCIAL_INSTITUTION_ADDR_TYPES.c_inst_addr_type_chn,
    ADDR_CODES.c_name AS c_inst_addr_pinyin,
    ADDR_CODES.c_name_chn AS c_inst_addr_chn
FROM
    (
        View_BiogInstData
        LEFT JOIN SOCIAL_INSTITUTION_ADDR_TYPES ON View_BiogInstData.c_inst_addr_type_code = SOCIAL_INSTITUTION_ADDR_TYPES.c_inst_addr_type_code
    )
    LEFT JOIN ADDR_CODES ON View_BiogInstData.c_inst_addr_id = ADDR_CODES.c_addr_id;
SQL
echo "Finished view View_BiogInstAddrData."

# View_BiogInstData: expands BIOG_INST_DATA with institution names, role codes,
# reign info, and coordinates.
echo "Creating view View_BiogInstData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogInstData;
CREATE VIEW View_BiogInstData AS
SELECT
    bi.c_personid,
    person.c_name,
    person.c_name_chn,
    bi.c_inst_name_code,
    bi.c_inst_code,
    inst_names.c_inst_name_hz,
    inst_names.c_inst_name_py,
    bi.c_bi_role_code,
    inst_codes.c_bi_role_desc,
    inst_codes.c_bi_role_chn,
    bi.c_bi_begin_year,
    bi.c_bi_by_nh_code,
    by_nh.c_nianhao_chn AS c_bi_by_nh_chn,
    by_nh.c_nianhao_pin AS c_bi_by_nh_py,
    bi.c_bi_by_nh_year,
    bi.c_bi_by_range,
    by_range.c_range AS c_bi_by_range_desc,
    by_range.c_range_chn AS c_bi_by_range_chn,
    bi.c_bi_end_year,
    bi.c_bi_ey_nh_code,
    ey_nh.c_nianhao_chn AS c_bi_ey_nh_chn,
    ey_nh.c_nianhao_pin AS c_bi_ey_nh_py,
    bi.c_bi_ey_nh_year,
    bi.c_bi_ey_range,
    ey_range.c_range AS c_bi_ey_range_desc,
    ey_range.c_range_chn AS c_bi_ey_range_chn,
    bi.c_source,
    text_codes.c_title_chn AS c_source_chn,
    text_codes.c_title AS c_source_py,
    bi.c_pages,
    bi.c_notes,
    inst_addr.c_inst_addr_id,
    inst_addr.c_inst_addr_type_code,
    inst_addr.inst_xcoord,
    inst_addr.inst_ycoord
FROM BIOG_INST_DATA AS bi
INNER JOIN BIOG_MAIN AS person
    ON person.c_personid = bi.c_personid
INNER JOIN SOCIAL_INSTITUTION_NAME_CODES AS inst_names
    ON inst_names.c_inst_name_code = bi.c_inst_name_code
INNER JOIN BIOG_INST_CODES AS inst_codes
    ON inst_codes.c_bi_role_code = bi.c_bi_role_code
LEFT JOIN NIAN_HAO AS by_nh
    ON by_nh.c_nianhao_id = bi.c_bi_by_nh_code
LEFT JOIN YEAR_RANGE_CODES AS by_range
    ON by_range.c_range_code = bi.c_bi_by_range
LEFT JOIN NIAN_HAO AS ey_nh
    ON ey_nh.c_nianhao_id = bi.c_bi_ey_nh_code
LEFT JOIN YEAR_RANGE_CODES AS ey_range
    ON ey_range.c_range_code = bi.c_bi_ey_range
LEFT JOIN TEXT_CODES AS text_codes
    ON text_codes.c_textid = bi.c_source
LEFT JOIN SOCIAL_INSTITUTION_ADDR AS inst_addr
    ON inst_addr.c_inst_name_code = bi.c_inst_name_code
   AND inst_addr.c_inst_code = bi.c_inst_code;
SQL
echo "Finished view View_BiogInstData."

# View_BiogSourceData: couples biographies with their textual sources and link
# metadata.
echo "Creating view View_BiogSourceData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogSourceData;
CREATE VIEW View_BiogSourceData AS
SELECT
    BIOG_SOURCE_DATA.c_personid AS c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    BIOG_SOURCE_DATA.c_textid,
    TEXT_CODES.c_title_chn,
    TEXT_CODES.c_title,
    BIOG_SOURCE_DATA.c_pages,
    TEXT_CODES.c_url_api,
    TEXT_CODES.c_url_api_coda,
    TEXT_CODES.c_url_homepage,
    BIOG_SOURCE_DATA.c_notes AS c_notes,
    BIOG_SOURCE_DATA.c_main_source,
    BIOG_SOURCE_DATA.c_self_bio AS c_self_bio,
    COALESCE(TEXT_CODES.c_url_api, '') || COALESCE(BIOG_SOURCE_DATA.c_pages, '') || COALESCE(TEXT_CODES.c_url_api_coda, '') AS c_hyperlink
FROM
    TEXT_CODES
    INNER JOIN (
        BIOG_MAIN
        INNER JOIN BIOG_SOURCE_DATA ON BIOG_MAIN.c_personid = BIOG_SOURCE_DATA.c_personid
    ) ON TEXT_CODES.c_textid = BIOG_SOURCE_DATA.c_textid;
SQL
echo "Finished view View_BiogSourceData."

# View_BiogTextData: links people, texts, and assigned roles.
echo "Creating view View_BiogTextData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogTextData;
CREATE VIEW View_BiogTextData AS
SELECT
    BIOG_TEXT_DATA.c_personid,
    BIOG_TEXT_DATA.c_textid AS c_textid,
    TEXT_CODES.c_title,
    TEXT_CODES.c_title_chn,
    BIOG_TEXT_DATA.c_role_id,
    TEXT_ROLE_CODES.c_role_desc,
    TEXT_ROLE_CODES.c_role_desc_chn,
    BIOG_TEXT_DATA.c_year,
    BIOG_TEXT_DATA.c_source AS c_source,
    TEXT_CODES_1.c_title AS c_source_title,
    TEXT_CODES_1.c_title_chn AS c_source_chn,
    BIOG_TEXT_DATA.c_pages AS c_pages,
    BIOG_TEXT_DATA.c_notes AS c_notes
FROM
    TEXT_ROLE_CODES
    INNER JOIN (
        TEXT_CODES
        INNER JOIN (
            BIOG_TEXT_DATA
            LEFT JOIN TEXT_CODES AS TEXT_CODES_1 ON BIOG_TEXT_DATA.c_source = TEXT_CODES_1.c_textid
        ) ON TEXT_CODES.c_textid = BIOG_TEXT_DATA.c_textid
    ) ON TEXT_ROLE_CODES.c_role_id = BIOG_TEXT_DATA.c_role_id;
SQL
echo "Finished view View_BiogTextData."

# View_Entry: covers exam/entry records with dynasties, kin, institutions, and
# location context.
echo "Creating view View_Entry..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_Entry;
CREATE VIEW View_Entry AS
SELECT
    entry.c_personid,
    person.c_name,
    person.c_name_chn,
    person.c_index_year,
    person.c_index_year_type_code,
    indexyear_codes.c_index_year_type_desc,
    indexyear_codes.c_index_year_type_hz,
    person.c_dy,
    dynasties.c_dynasty,
    dynasties.c_dynasty_chn,
    entry.c_entry_code,
    entry_codes.c_entry_desc,
    entry_codes.c_entry_desc_chn,
    entry.c_year,
    entry.c_sequence,
    person.c_index_addr_id,
    index_addr.c_name AS c_addr_name,
    index_addr.c_name_chn AS c_addr_chn,
    index_addr.x_coord,
    index_addr.y_coord,
    person.c_index_addr_type_code,
    index_addr_type.c_addr_desc,
    index_addr_type.c_addr_desc_chn,
    entry.c_exam_rank,
    entry.c_kin_code,
    kin_codes.c_kinrel_chn,
    kin_codes.c_kinrel,
    entry.c_kin_id,
    kin_person.c_name AS c_kin_name,
    kin_person.c_name_chn AS c_kin_name_chn,
    entry.c_assoc_code,
    assoc_codes.c_assoc_desc,
    assoc_codes.c_assoc_desc_chn,
    entry.c_assoc_id,
    assoc_person.c_name AS c_assoc_name,
    assoc_person.c_name_chn AS c_assoc_name_chn,
    entry.c_age,
    entry.c_nianhao_id,
    nh.c_nianhao_chn,
    nh.c_nianhao_pin,
    entry.c_entry_nh_year,
    entry.c_entry_range,
    range_codes.c_range,
    range_codes.c_range_chn,
    entry.c_inst_code,
    entry.c_inst_name_code,
    inst_names.c_inst_name_hz,
    inst_names.c_inst_name_py,
    entry.c_exam_field,
    entry.c_entry_addr_id,
    entry_addr.c_name AS c_entry_addr_name,
    entry_addr.c_name_chn AS c_entry_addr_chn,
    entry_addr.x_coord AS c_entry_xcoord,
    entry_addr.y_coord AS c_entry_ycoord,
    entry.c_parental_status,
    parental_codes.c_parental_status_desc,
    parental_codes.c_parental_status_desc_chn,
    entry.c_attempt_count,
    entry.c_source,
    text_codes.c_title,
    text_codes.c_title_chn,
    entry.c_pages,
    entry.c_notes,
    entry.c_posting_notes
FROM ENTRY_DATA AS entry
INNER JOIN ENTRY_CODES AS entry_codes
    ON entry_codes.c_entry_code = entry.c_entry_code
INNER JOIN BIOG_MAIN AS person
    ON person.c_personid = entry.c_personid
LEFT JOIN BIOG_MAIN AS kin_person
    ON kin_person.c_personid = entry.c_kin_id
LEFT JOIN BIOG_MAIN AS assoc_person
    ON assoc_person.c_personid = entry.c_assoc_id
LEFT JOIN KINSHIP_CODES AS kin_codes
    ON kin_codes.c_kincode = entry.c_kin_code
LEFT JOIN ASSOC_CODES AS assoc_codes
    ON assoc_codes.c_assoc_code = entry.c_assoc_code
LEFT JOIN PARENTAL_STATUS_CODES AS parental_codes
    ON parental_codes.c_parental_status_code = entry.c_parental_status
LEFT JOIN NIAN_HAO AS nh
    ON nh.c_nianhao_id = entry.c_nianhao_id
LEFT JOIN YEAR_RANGE_CODES AS range_codes
    ON range_codes.c_range_code = entry.c_entry_range
LEFT JOIN ADDR_CODES AS entry_addr
    ON entry_addr.c_addr_id = entry.c_entry_addr_id
LEFT JOIN TEXT_CODES AS text_codes
    ON text_codes.c_textid = entry.c_source
LEFT JOIN SOCIAL_INSTITUTION_NAME_CODES AS inst_names
    ON inst_names.c_inst_name_code = entry.c_inst_name_code
LEFT JOIN ADDR_CODES AS index_addr
    ON index_addr.c_addr_id = person.c_index_addr_id
LEFT JOIN BIOG_ADDR_CODES AS index_addr_type
    ON index_addr_type.c_addr_type = person.c_index_addr_type_code
LEFT JOIN INDEXYEAR_TYPE_CODES AS indexyear_codes
    ON indexyear_codes.c_index_year_type_code = person.c_index_year_type_code
LEFT JOIN DYNASTIES AS dynasties
    ON dynasties.c_dy = person.c_dy;
SQL
echo "Finished view View_Entry."

# View_EventAddr & View_EventData: combine event participation with address,
# reign, and text info.
echo "Creating view View_EventAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_EventAddr;
CREATE VIEW View_EventAddr AS
SELECT
    ea.c_personid,
    person.c_name AS c_person_name,
    person.c_name_chn AS c_person_chn,
    event_data.c_event_code,
    event_codes.c_event_name_chn,
    event_codes.c_event_name,
    event_data.c_sequence,
    ea.c_addr_id,
    addr.c_name AS c_event_addr_name,
    addr.c_name_chn AS c_event_addr_chn,
    addr.x_coord AS c_event_xcoord,
    addr.y_coord AS c_event_ycoord,
    ea.c_year,
    ea.c_nh_code,
    nh.c_nianhao_chn,
    nh.c_nianhao_pin,
    ea.c_nh_year,
    ea.c_yr_range,
    range_codes.c_range,
    range_codes.c_range_chn,
    ea.c_intercalary,
    ea.c_month,
    ea.c_day,
    ea.c_day_ganzhi,
    gz.c_ganzhi_chn AS c_event_day_gz_chn,
    gz.c_ganzhi_py AS c_event_day_gz_py
FROM EVENTS_ADDR AS ea
LEFT JOIN EVENTS_DATA AS event_data
    ON event_data.c_event_record_id = ea.c_event_record_id
   AND event_data.c_personid = ea.c_personid
LEFT JOIN EVENT_CODES AS event_codes
    ON event_codes.c_event_code = event_data.c_event_code
INNER JOIN BIOG_MAIN AS person
    ON person.c_personid = ea.c_personid
LEFT JOIN ADDR_CODES AS addr
    ON addr.c_addr_id = ea.c_addr_id
LEFT JOIN GANZHI_CODES AS gz
    ON gz.c_ganzhi_code = ea.c_day_ganzhi
LEFT JOIN YEAR_RANGE_CODES AS range_codes
    ON range_codes.c_range_code = ea.c_yr_range
LEFT JOIN NIAN_HAO AS nh
    ON nh.c_nianhao_id = ea.c_nh_code;
SQL
echo "Finished view View_EventAddr."

# See comment above.
echo "Creating view View_EventData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_EventData;
CREATE VIEW View_EventData AS
SELECT
    ed.c_personid,
    person.c_name,
    person.c_name_chn,
    ed.c_sequence,
    ed.c_event_code,
    event_codes.c_event_name_chn,
    event_codes.c_event_name,
    ed.c_role,
    ed.c_year,
    ed.c_nh_code,
    nh.c_nianhao_chn,
    nh.c_nianhao_pin,
    ed.c_nh_year,
    ed.c_yr_range,
    range_codes.c_range,
    range_codes.c_range_chn,
    ed.c_intercalary,
    ed.c_month,
    ed.c_day,
    ed.c_day_ganzhi,
    gz.c_ganzhi_chn AS c_event_day_gz_chn,
    gz.c_ganzhi_py AS c_event_day_gz_py,
    ed.c_source,
    texts.c_title AS c_source_title,
    texts.c_title_chn AS c_source_chn,
    ed.c_pages,
    ed.c_notes,
    NULL AS c_person_text_title,
    NULL AS c_person_text_pages,
    NULL AS c_person_text_notes
FROM EVENTS_DATA AS ed
INNER JOIN EVENT_CODES AS event_codes
    ON event_codes.c_event_code = ed.c_event_code
INNER JOIN BIOG_MAIN AS person
    ON person.c_personid = ed.c_personid
LEFT JOIN NIAN_HAO AS nh
    ON nh.c_nianhao_id = ed.c_nh_code
LEFT JOIN YEAR_RANGE_CODES AS range_codes
    ON range_codes.c_range_code = ed.c_yr_range
LEFT JOIN GANZHI_CODES AS gz
    ON gz.c_ganzhi_code = ed.c_day_ganzhi
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = ed.c_source;
SQL
echo "Finished view View_EventData."

# View_KinAddr: exposes kin relations with address and citation data.
echo "Creating view View_KinAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_KinAddr;
CREATE VIEW View_KinAddr AS
SELECT
    kd.c_personid,
    person.c_name,
    person.c_name_chn,
    kd.c_kin_id,
    kin_person.c_name AS c_kin_name,
    kin_person.c_name_chn AS c_kin_chn,
    kd.c_kin_code,
    kin_codes.c_kinrel,
    kin_codes.c_kinrel_chn,
    NULL AS c_addr_name,
    NULL AS c_addr_chn,
    kd.c_source,
    texts.c_title,
    texts.c_title_chn,
    kd.c_pages,
    kd.c_notes,
    NULL AS c_sequence
FROM KIN_DATA AS kd
INNER JOIN BIOG_MAIN AS person
    ON person.c_personid = kd.c_personid
LEFT JOIN BIOG_MAIN AS kin_person
    ON kin_person.c_personid = kd.c_kin_id
LEFT JOIN KINSHIP_CODES AS kin_codes
    ON kin_codes.c_kincode = kd.c_kin_code
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = kd.c_source;
SQL
echo "Finished view View_KinAddr."

# View_People: produces a denormalized “person dossier” with birth/death reign
# data, ethnicity, household, choronym, and textual references.
echo "Creating view View_People..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_People;
CREATE VIEW View_People AS
SELECT
    bm.c_personid,
    bm.c_name,
    bm.c_name_chn,
    bm.c_index_year,
    bm.c_index_year_type_code,
    indexyear_codes.c_index_year_type_desc,
    indexyear_codes.c_index_year_type_hz,
    bm.c_index_year_source_id,
    index_source.c_title AS c_source_title,
    index_source.c_title_chn AS c_source_chn,
    bm.c_female,
    bm.c_index_addr_id,
    index_addr.c_name AS c_index_addr_name,
    index_addr.c_name_chn AS c_index_addr_chn,
    index_addr.x_coord AS c_index_addr_x_coord,
    index_addr.y_coord AS c_index_addr_y_coord,
    bm.c_index_addr_type_code,
    index_addr_type.c_addr_desc,
    index_addr_type.c_addr_desc_chn,
    bm.c_ethnicity_code,
    ethnicity.c_name AS c_ethnicity_desc,
    ethnicity.c_name_chn AS c_ethnicity_desc_chn,
    bm.c_household_status_code,
    household.c_household_status_desc,
    household.c_household_status_desc_chn,
    bm.c_tribe,
    bm.c_birthyear,
    bm.c_by_nh_code,
    by_nh.c_nianhao_chn AS c_by_nh_chn,
    by_nh.c_nianhao_pin AS c_by_nh_py,
    bm.c_by_nh_year,
    bm.c_by_range,
    by_range.c_range AS c_by_range_desc,
    by_range.c_range_chn AS c_by_range_chn,
    bm.c_deathyear,
    bm.c_dy_nh_code,
    dy_nh.c_nianhao_chn AS c_dy_nh_chn,
    dy_nh.c_nianhao_pin AS c_dy_nh_py,
    bm.c_dy_nh_year,
    bm.c_dy_range,
    dy_range.c_range AS c_dy_range_desc,
    dy_range.c_range_chn AS c_dy_range_chn,
    bm.c_death_age,
    bm.c_death_age_range,
    death_age_range.c_range AS c_death_age_range_desc,
    death_age_range.c_range_chn AS c_death_age_range_chn,
    bm.c_fl_earliest_year,
    bm.c_fl_ey_nh_code,
    fl_ey_nh.c_nianhao_chn AS c_fl_ey_nh_chn,
    fl_ey_nh.c_nianhao_pin AS c_fl_ey_nh_py,
    bm.c_fl_ey_nh_year,
    NULL AS c_fl_ey_range,
    bm.c_fl_latest_year,
    bm.c_fl_ly_nh_code,
    fl_ly_nh.c_nianhao_chn AS c_fl_ly_nh_chn,
    fl_ly_nh.c_nianhao_pin AS c_fl_ly_nh_py,
    bm.c_fl_ly_nh_year,
    NULL AS c_fl_ly_range,
    bm.c_surname,
    bm.c_surname_chn,
    bm.c_mingzi,
    bm.c_mingzi_chn,
    bm.c_dy,
    dynasties.c_dynasty,
    dynasties.c_dynasty_chn,
    bm.c_choronym_code,
    choronym.c_choronym_desc,
    choronym.c_choronym_chn AS c_choronym_desc_chn,
    bm.c_notes,
    bm.c_by_intercalary,
    bm.c_dy_intercalary,
    bm.c_by_month,
    bm.c_dy_month,
    bm.c_by_day,
    bm.c_dy_day,
    bm.c_by_day_gz,
    by_gz.c_ganzhi_chn AS c_by_day_gz_chn,
    by_gz.c_ganzhi_py AS c_by_day_gz_py,
    bm.c_dy_day_gz,
    dy_gz.c_ganzhi_chn AS c_dy_day_gz_chn,
    dy_gz.c_ganzhi_py AS c_dy_day_gz_py,
    bm.c_surname_proper,
    bm.c_mingzi_proper,
    bm.c_name_proper,
    bm.c_surname_rm,
    bm.c_mingzi_rm,
    bm.c_name_rm,
    bm.c_created_by,
    bm.c_created_date,
    bm.c_modified_by,
    bm.c_modified_date,
    bm.c_self_bio
FROM BIOG_MAIN AS bm
LEFT JOIN INDEXYEAR_TYPE_CODES AS indexyear_codes
    ON indexyear_codes.c_index_year_type_code = bm.c_index_year_type_code
LEFT JOIN TEXT_CODES AS index_source
    ON index_source.c_textid = bm.c_index_year_source_id
LEFT JOIN ADDR_CODES AS index_addr
    ON index_addr.c_addr_id = bm.c_index_addr_id
LEFT JOIN BIOG_ADDR_CODES AS index_addr_type
    ON index_addr_type.c_addr_type = bm.c_index_addr_type_code
LEFT JOIN ETHNICITY_TRIBE_CODES AS ethnicity
    ON ethnicity.c_ethnicity_code = bm.c_ethnicity_code
LEFT JOIN HOUSEHOLD_STATUS_CODES AS household
    ON household.c_household_status_code = bm.c_household_status_code
LEFT JOIN NIAN_HAO AS by_nh
    ON by_nh.c_nianhao_id = bm.c_by_nh_code
LEFT JOIN YEAR_RANGE_CODES AS by_range
    ON by_range.c_range_code = bm.c_by_range
LEFT JOIN NIAN_HAO AS dy_nh
    ON dy_nh.c_nianhao_id = bm.c_dy_nh_code
LEFT JOIN YEAR_RANGE_CODES AS dy_range
    ON dy_range.c_range_code = bm.c_dy_range
LEFT JOIN YEAR_RANGE_CODES AS death_age_range
    ON death_age_range.c_range_code = bm.c_death_age_range
LEFT JOIN NIAN_HAO AS fl_ey_nh
    ON fl_ey_nh.c_nianhao_id = bm.c_fl_ey_nh_code
LEFT JOIN NIAN_HAO AS fl_ly_nh
    ON fl_ly_nh.c_nianhao_id = bm.c_fl_ly_nh_code
LEFT JOIN DYNASTIES AS dynasties
    ON dynasties.c_dy = bm.c_dy
LEFT JOIN CHORONYM_CODES AS choronym
    ON choronym.c_choronym_code = bm.c_choronym_code
LEFT JOIN GANZHI_CODES AS by_gz
    ON by_gz.c_ganzhi_code = bm.c_by_day_gz
LEFT JOIN GANZHI_CODES AS dy_gz
    ON dy_gz.c_ganzhi_code = bm.c_dy_day_gz;
SQL
echo "Finished view View_People."

# View_PeopleAddr: pares this down to index addresses.
echo "Creating view View_PeopleAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_PeopleAddr;
CREATE VIEW View_PeopleAddr AS
SELECT
    BIOG_MAIN.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    BIOG_MAIN.c_index_year,
    BIOG_MAIN.c_female,
    BIOG_MAIN.c_index_addr_id,
    BIOG_MAIN.c_index_addr_type_code,
    ADDR_CODES.c_name AS c_index_addr_name,
    ADDR_CODES.c_name_chn AS c_index_addr_chn,
    BIOG_ADDR_CODES.c_addr_desc AS c_index_addr_type_desc,
    BIOG_ADDR_CODES.c_addr_desc_chn AS c_index_addr_type_chn,
    ADDR_CODES.x_coord,
    ADDR_CODES.y_coord
FROM
    (
        BIOG_MAIN
        LEFT JOIN ADDR_CODES ON BIOG_MAIN.c_index_addr_id = ADDR_CODES.c_addr_id
    )
    LEFT JOIN BIOG_ADDR_CODES ON BIOG_MAIN.c_index_addr_type_code = BIOG_ADDR_CODES.c_addr_type;
SQL
echo "Finished view View_PeopleAddr."

# View_Possessions: joins possession records to act/measure codes, reign data,
# and texts; View_PossessionsAddr adds the resolved address.
echo "Creating view View_Possessions..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_Possessions;
CREATE VIEW View_Possessions AS
SELECT
    pd.c_personid,
    pd.c_possession_record_id,
    pd.c_sequence,
    pd.c_possession_act_code,
    act_codes.c_possession_act_desc,
    act_codes.c_possession_act_desc_chn,
    pd.c_possession_desc,
    pd.c_possession_desc_chn,
    pd.c_quantity,
    pd.c_measure_code,
    measure_codes.c_measure_desc,
    measure_codes.c_measure_desc_chn,
    pd.c_possession_yr,
    pd.c_possession_nh_code,
    nh.c_nianhao_chn,
    nh.c_nianhao_pin,
    pd.c_possession_nh_yr,
    pd.c_possession_yr_range,
    range_codes.c_range,
    range_codes.c_range_chn,
    pd.c_source,
    texts.c_title_chn,
    texts.c_title,
    pd.c_pages,
    pd.c_notes,
    addr.c_addr_id
FROM POSSESSION_DATA AS pd
LEFT JOIN POSSESSION_ACT_CODES AS act_codes
    ON act_codes.c_possession_act_code = pd.c_possession_act_code
LEFT JOIN MEASURE_CODES AS measure_codes
    ON measure_codes.c_measure_code = pd.c_measure_code
LEFT JOIN NIAN_HAO AS nh
    ON nh.c_nianhao_id = pd.c_possession_nh_code
LEFT JOIN YEAR_RANGE_CODES AS range_codes
    ON range_codes.c_range_code = pd.c_possession_yr_range
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = pd.c_source
LEFT JOIN POSSESSION_ADDR AS addr
    ON addr.c_possession_record_id = pd.c_possession_record_id;
SQL
echo "Finished view View_Possessions."

# See above.
echo "Creating view View_PossessionsAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_PossessionsAddr;
CREATE VIEW View_PossessionsAddr AS
SELECT
    View_Possessions.*,
    ADDR_CODES.c_name AS c_addr_name,
    ADDR_CODES.c_name_chn AS c_addr_chn
FROM
    View_Possessions
    LEFT JOIN ADDR_CODES ON View_Possessions.c_addr_id = ADDR_CODES.c_addr_id;
SQL
echo "Finished view View_PossessionsAddr."

# View_PostingAddr & View_PostingOffice: wrap office postings with addresses,
# office metadata, dynasties, and reign data.
echo "Creating view View_PostingAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_PostingAddr;
CREATE VIEW View_PostingAddr AS
SELECT
    POSTED_TO_ADDR_DATA.c_personid,
    POSTED_TO_ADDR_DATA.c_posting_id,
    POSTED_TO_ADDR_DATA.c_office_id,
    POSTED_TO_ADDR_DATA.c_addr_id,
    ADDR_CODES.c_name AS c_office_addr_name,
    ADDR_CODES.c_name_chn AS c_office_addr_chn
FROM
    ADDR_CODES
    INNER JOIN POSTED_TO_ADDR_DATA ON ADDR_CODES.c_addr_id = POSTED_TO_ADDR_DATA.c_addr_id;
SQL
echo "Finished view View_PostingAddr."

# See above.
echo "Creating view View_PostingOffice..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_PostingOffice;
CREATE VIEW View_PostingOffice AS
SELECT
    po.c_personid,
    po.c_office_id,
    office.c_office_pinyin,
    office.c_office_chn,
    office.c_office_trans,
    po.c_posting_id,
    po.c_sequence,
    po.c_firstyear,
    po.c_fy_nh_code,
    fy_nh.c_nianhao_chn AS c_fy_nh_chn,
    fy_nh.c_nianhao_pin AS c_fy_nh_py,
    po.c_fy_nh_year,
    po.c_fy_range,
    fy_range.c_range AS c_fy_range_desc,
    fy_range.c_range_chn AS c_fy_range_chn,
    po.c_lastyear,
    po.c_ly_nh_code,
    ly_nh.c_nianhao_chn AS c_ly_nh_chn,
    ly_nh.c_nianhao_pin AS c_ly_nh_py,
    po.c_ly_nh_year,
    po.c_ly_range,
    ly_range.c_range AS c_ly_range_desc,
    ly_range.c_range_chn AS c_ly_range_chn,
    po.c_appt_code,
    appt_codes.c_appt_desc_chn,
    appt_codes.c_appt_desc,
    po.c_assume_office_code,
    assume_codes.c_assume_office_desc_chn,
    assume_codes.c_assume_office_desc,
    po.c_inst_code,
    po.c_inst_name_code,
    inst_names.c_inst_name_hz,
    inst_names.c_inst_name_py,
    po.c_source,
    texts.c_title_chn,
    texts.c_title,
    po.c_pages,
    po.c_notes,
    po.c_office_category_id,
    categories.c_category_desc,
    categories.c_category_desc_chn,
    po.c_fy_intercalary,
    po.c_fy_month,
    po.c_ly_intercalary,
    po.c_ly_month,
    po.c_fy_day,
    po.c_ly_day,
    po.c_fy_day_gz,
    fy_gz.c_ganzhi_chn AS c_fy_day_gz_chn,
    fy_gz.c_ganzhi_py AS c_fy_day_gz_py,
    po.c_ly_day_gz,
    ly_gz.c_ganzhi_chn AS c_ly_day_gz_chn,
    ly_gz.c_ganzhi_py AS c_ly_day_gz_py,
    po.c_dy,
    dynasties.c_dynasty,
    dynasties.c_dynasty_chn
FROM POSTED_TO_OFFICE_DATA AS po
INNER JOIN OFFICE_CODES AS office
    ON office.c_office_id = po.c_office_id
INNER JOIN BIOG_MAIN AS person
    ON person.c_personid = po.c_personid
LEFT JOIN SOCIAL_INSTITUTION_NAME_CODES AS inst_names
    ON inst_names.c_inst_name_code = po.c_inst_name_code
LEFT JOIN APPOINTMENT_CODES AS appt_codes
    ON appt_codes.c_appt_code = po.c_appt_code
LEFT JOIN ASSUME_OFFICE_CODES AS assume_codes
    ON assume_codes.c_assume_office_code = po.c_assume_office_code
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = po.c_source
LEFT JOIN GANZHI_CODES AS fy_gz
    ON fy_gz.c_ganzhi_code = po.c_fy_day_gz
LEFT JOIN GANZHI_CODES AS ly_gz
    ON ly_gz.c_ganzhi_code = po.c_ly_day_gz
LEFT JOIN YEAR_RANGE_CODES AS fy_range
    ON fy_range.c_range_code = po.c_fy_range
LEFT JOIN YEAR_RANGE_CODES AS ly_range
    ON ly_range.c_range_code = po.c_ly_range
LEFT JOIN NIAN_HAO AS fy_nh
    ON fy_nh.c_nianhao_id = po.c_fy_nh_code
LEFT JOIN NIAN_HAO AS ly_nh
    ON ly_nh.c_nianhao_id = po.c_ly_nh_code
LEFT JOIN OFFICE_CATEGORIES AS categories
    ON categories.c_office_category_id = po.c_office_category_id
LEFT JOIN DYNASTIES AS dynasties
    ON dynasties.c_dy = po.c_dy;
SQL
echo "Finished view View_PostingOffice."

# View_StatusData: assembles status changes with reign spans and document
# metadata.
echo "Creating view View_StatusData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_StatusData;
CREATE VIEW View_StatusData AS
SELECT
    sd.c_personid,
    sd.c_sequence,
    sd.c_status_code,
    status_codes.c_status_desc,
    status_codes.c_status_desc_chn,
    sd.c_firstyear,
    sd.c_fy_nh_code,
    fy_nh.c_nianhao_chn AS c_fy_nh_chn,
    fy_nh.c_nianhao_pin AS c_fy_nh_py,
    sd.c_fy_nh_year,
    sd.c_fy_range,
    fy_range.c_range AS c_fy_range_desc,
    fy_range.c_range_chn AS c_fy_range_chn,
    sd.c_lastyear,
    sd.c_ly_nh_code,
    ly_nh.c_nianhao_chn AS c_ly_nh_chn,
    ly_nh.c_nianhao_pin AS c_ly_nh_py,
    sd.c_ly_nh_year,
    sd.c_ly_range,
    ly_range.c_range AS c_ly_range_desc,
    ly_range.c_range_chn AS c_ly_range_chn,
    sd.c_supplement,
    sd.c_source,
    texts.c_title_chn,
    texts.c_title,
    sd.c_pages,
    sd.c_notes
FROM STATUS_DATA AS sd
INNER JOIN STATUS_CODES AS status_codes
    ON status_codes.c_status_code = sd.c_status_code
LEFT JOIN NIAN_HAO AS fy_nh
    ON fy_nh.c_nianhao_id = sd.c_fy_nh_code
LEFT JOIN YEAR_RANGE_CODES AS fy_range
    ON fy_range.c_range_code = sd.c_fy_range
LEFT JOIN NIAN_HAO AS ly_nh
    ON ly_nh.c_nianhao_id = sd.c_ly_nh_code
LEFT JOIN YEAR_RANGE_CODES AS ly_range
    ON ly_range.c_range_code = sd.c_ly_range
LEFT JOIN TEXT_CODES AS texts
    ON texts.c_textid = sd.c_source;
SQL
echo "Finished view View_StatusData."

echo "Created CBDB views in '$DB_PATH'."

echo "Running sanity counts on views..."

# List of views to check
VIEWS=(
    "View_AltnameData"
    "View_Association"
    "View_BiogAddrData"
    "View_BiogInstAddrData"
    "View_BiogInstData"
    "View_BiogSourceData"
    "View_BiogTextData"
    "View_Entry"
    "View_EventAddr"
    "View_EventData"
    "View_KinAddr"
    "View_People"
    "View_PeopleAddr"
    "View_Possessions"
    "View_PossessionsAddr"
    "View_PostingAddr"
    "View_PostingOffice"
    "View_StatusData"
)

# Check each view individually to identify which one causes issues
for view in "${VIEWS[@]}"; do
    echo "Checking view: $view..."
    if sqlite3 "$DB_PATH" "SELECT '$view' AS view_name, COUNT(*) AS row_count FROM $view;" 2>&1; then
        echo "  ✓ $view completed successfully"
    else
        EXIT_CODE=$?
        echo "  ✗ ERROR: $view failed with exit code $EXIT_CODE"
        echo "  Memory info:"
        free -h 2>/dev/null || true
        exit $EXIT_CODE
    fi
done

echo "All sanity checks passed!"
