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
CREATE VIEW View_AltnameData as
SELECT
    ALTNAME_DATA.c_personid,
    ALTNAME_DATA.c_alt_name,
    ALTNAME_DATA.c_alt_name_chn,
    ALTNAME_DATA.c_alt_name_type_code,
    ALTNAME_CODES.c_name_type_desc,
    ALTNAME_CODES.c_name_type_desc_chn,
    ALTNAME_DATA.c_sequence,
    ALTNAME_DATA.c_source,
    TEXT_CODES.c_title,
    TEXT_CODES.c_title_chn,
    ALTNAME_DATA.c_pages,
    ALTNAME_DATA.c_notes
FROM
    TEXT_CODES
    RIGHT JOIN (
        ALTNAME_CODES
        INNER JOIN ALTNAME_DATA ON ALTNAME_CODES.c_name_type_code = ALTNAME_DATA.c_alt_name_type_code
    ) ON TEXT_CODES.c_textid = ALTNAME_DATA.c_source;
SQL
echo "Finished view View_AltnameData."

# View_Association: fan-out join on ASSOC_DATA to kinship, institution, genre,
# occasion, topic, address, and textual sources.
echo "Creating view View_Association..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_Association;
CREATE VIEW View_Association As
SELECT
    ASSOC_DATA.c_personid,
    ASSOC_DATA.c_assoc_id AS c_node_id,
    BIOG_MAIN_5.c_name AS c_node_name,
    BIOG_MAIN_5.c_name_chn AS c_node_chn,
    ASSOC_DATA.c_assoc_code AS c_link_code,
    ASSOC_CODES.c_assoc_desc AS c_link_desc,
    ASSOC_CODES.c_assoc_desc_chn AS c_link_chn,
    ASSOC_DATA.c_kin_code,
    KINSHIP_CODES.c_kinrel_chn,
    KINSHIP_CODES.c_kinrel,
    BIOG_MAIN_1.c_name AS c_kin_name,
    BIOG_MAIN_1.c_name_chn AS c_kin_chn,
    ASSOC_DATA.c_assoc_kin_id,
    KINSHIP_CODES_1.c_kinrel_chn AS c_assoc_kinrel_chn,
    KINSHIP_CODES_1.c_kinrel AS c_assoc_kinrel,
    BIOG_MAIN_2.c_name AS c_assoc_kin_name,
    BIOG_MAIN_2.c_name_chn AS c_assoc_kin_chn,
    LITERARYGENRE_CODES.c_lit_genre_desc,
    LITERARYGENRE_CODES.c_lit_genre_desc_chn,
    OCCASION_CODES.c_occasion_desc,
    OCCASION_CODES.c_occasion_desc_chn,
    SCHOLARLYTOPIC_CODES.c_topic_desc,
    SCHOLARLYTOPIC_CODES.c_topic_desc_chn,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_py,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_hz,
    ASSOC_DATA.c_text_title,
    ASSOC_DATA.c_assoc_claimer_id,
    BIOG_MAIN_4.c_name AS C_assoc_claimer_name,
    BIOG_MAIN_4.c_name_chn AS c_assoc_claimer_chn,
    TEXT_CODES.c_title AS c_source_title,
    TEXT_CODES.c_title_chn AS c_source_chn,
    ASSOC_DATA.c_notes,
    ASSOC_DATA.c_pages,
    ASSOC_DATA.c_sequence,
    ASSOC_DATA.c_assoc_count AS c_link_count,
    ADDR_CODES.c_name AS c_assoc_addr_name,
    ADDR_CODES.c_name_chn AS c_assoc_addr_chn,
    ASSOC_DATA.c_assoc_first_year,
    YEAR_RANGE_CODES.c_range,
    YEAR_RANGE_CODES.c_range_chn,
    ASSOC_DATA.c_assoc_fy_intercalary,
    ASSOC_DATA.c_assoc_fy_month,
    ASSOC_DATA.c_assoc_fy_day,
    NIAN_HAO.c_nianhao_chn AS c_assoc_fy_nh_chn,
    NIAN_HAO.c_nianhao_pin AS c_assoc_fy_nh_py,
    ASSOC_DATA.c_assoc_fy_nh_year,
    GANZHI_CODES.c_ganzhi_chn,
    GANZHI_CODES.c_ganzhi_py
FROM
    (
        KINSHIP_CODES AS KINSHIP_CODES_1
        RIGHT JOIN (
            (
                (
                    SCHOLARLYTOPIC_CODES
                    RIGHT JOIN (
                        OCCASION_CODES
                        RIGHT JOIN (
                            LITERARYGENRE_CODES
                            RIGHT JOIN (
                                ADDR_CODES
                                RIGHT JOIN (
                                    GANZHI_CODES
                                    RIGHT JOIN (
                                        YEAR_RANGE_CODES
                                        RIGHT JOIN (
                                            NIAN_HAO
                                            RIGHT JOIN (
                                                TEXT_CODES
                                                RIGHT JOIN (
                                                    (
                                                        (
                                                            KINSHIP_CODES
                                                            RIGHT JOIN (
                                                                ASSOC_CODES
                                                                INNER JOIN ASSOC_DATA ON ASSOC_CODES.c_assoc_code = ASSOC_DATA.c_assoc_code
                                                            ) ON KINSHIP_CODES.c_kincode = ASSOC_DATA.c_kin_code
                                                        )
                                                        LEFT JOIN BIOG_MAIN AS BIOG_MAIN_1 ON ASSOC_DATA.c_kin_id = BIOG_MAIN_1.c_personid
                                                    )
                                                    LEFT JOIN BIOG_MAIN AS BIOG_MAIN_2 ON ASSOC_DATA.c_assoc_kin_id = BIOG_MAIN_2.c_personid
                                                ) ON TEXT_CODES.c_textid = ASSOC_DATA.c_source
                                            ) ON NIAN_HAO.c_nianhao_id = ASSOC_DATA.c_assoc_fy_nh_code
                                        ) ON YEAR_RANGE_CODES.c_range_code = ASSOC_DATA.c_assoc_fy_range
                                    ) ON GANZHI_CODES.c_ganzhi_code = ASSOC_DATA.c_assoc_fy_day_gz
                                ) ON ADDR_CODES.c_addr_id = ASSOC_DATA.c_addr_id
                            ) ON LITERARYGENRE_CODES.c_lit_genre_code = ASSOC_DATA.c_litgenre_code
                        ) ON OCCASION_CODES.c_occasion_code = ASSOC_DATA.c_occasion_code
                    ) ON SCHOLARLYTOPIC_CODES.c_topic_code = ASSOC_DATA.c_topic_code
                )
                LEFT JOIN BIOG_MAIN AS BIOG_MAIN_4 ON ASSOC_DATA.c_assoc_claimer_id = BIOG_MAIN_4.c_personid
            )
            INNER JOIN BIOG_MAIN AS BIOG_MAIN_5 ON ASSOC_DATA.c_assoc_id = BIOG_MAIN_5.c_personid
        ) ON KINSHIP_CODES_1.c_kincode = ASSOC_DATA.c_assoc_kin_code
    )
    LEFT JOIN SOCIAL_INSTITUTION_NAME_CODES ON ASSOC_DATA.c_inst_name_code = SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_code;
SQL
echo "Finished view View_Association."

# View_BiogAddrData: enriches BIOG_ADDR_DATA with address labels, reign data,
# ganzhi, and text citations.
echo "Creating view View_BiogAddrData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogAddrData;
CREATE VIEW View_BiogAddrData AS
SELECT
    BIOG_ADDR_DATA.c_personid,
    BIOG_ADDR_DATA.c_addr_id,
    ADDR_CODES.c_name AS c_addr_name,
    ADDR_CODES.c_name_chn AS c_addr_chn,
    BIOG_ADDR_CODES.c_addr_desc,
    BIOG_ADDR_CODES.c_addr_desc_chn,
    BIOG_ADDR_DATA.c_firstyear,
    BIOG_ADDR_DATA.c_lastyear,
    BIOG_ADDR_DATA.c_source,
    TEXT_CODES.c_title_chn AS c_source_chn,
    TEXT_CODES.c_title AS c_source_title,
    BIOG_ADDR_DATA.c_pages,
    BIOG_ADDR_DATA.c_notes,
    NIAN_HAO.c_nianhao_chn AS c_fy_nh_chn,
    NIAN_HAO.c_nianhao_pin AS c_fy_nh_py,
    BIOG_ADDR_DATA.c_fy_nh_year,
    BIOG_ADDR_DATA.c_fy_month,
    BIOG_ADDR_DATA.c_fy_day,
    GANZHI_CODES.c_ganzhi_chn AS c_fy_day_gz_chn,
    GANZHI_CODES.c_ganzhi_py AS c_fy_day_gz_py,
    BIOG_ADDR_DATA.c_fy_intercalary,
    YEAR_RANGE_CODES.c_range AS c_fy_range_desc,
    YEAR_RANGE_CODES.c_range_chn AS c_fy_range_chn,
    NIAN_HAO_1.c_nianhao_chn AS c_ly_nh_chn,
    NIAN_HAO_1.c_nianhao_pin AS c_ly_nh_py,
    BIOG_ADDR_DATA.c_ly_nh_year,
    BIOG_ADDR_DATA.c_ly_intercalary,
    BIOG_ADDR_DATA.c_ly_month,
    BIOG_ADDR_DATA.c_ly_day,
    GANZHI_CODES_1.c_ganzhi_chn AS c_ly_day_gz_chn,
    GANZHI_CODES_1.c_ganzhi_py AS c_ly_day_gz_py,
    YEAR_RANGE_CODES_1.c_range AS c_ly_range_desc,
    YEAR_RANGE_CODES_1.c_range_chn AS c_ly_range_chn,
    BIOG_ADDR_DATA.c_natal,
    BIOG_ADDR_DATA.c_fy_nh_code,
    BIOG_ADDR_DATA.c_ly_nh_code,
    BIOG_ADDR_DATA.c_sequence
FROM
    (
        (
            (
                (
                    NIAN_HAO AS NIAN_HAO_1
                    RIGHT JOIN (
                        NIAN_HAO
                        RIGHT JOIN (
                            (
                                (
                                    BIOG_ADDR_CODES
                                    INNER JOIN BIOG_ADDR_DATA ON BIOG_ADDR_CODES.c_addr_type = BIOG_ADDR_DATA.c_addr_type
                                )
                                INNER JOIN ADDR_CODES ON BIOG_ADDR_DATA.c_addr_id = ADDR_CODES.c_addr_id
                            )
                            LEFT JOIN TEXT_CODES ON BIOG_ADDR_DATA.c_source = TEXT_CODES.c_textid
                        ) ON NIAN_HAO.c_nianhao_id = BIOG_ADDR_DATA.c_fy_nh_code
                    ) ON NIAN_HAO_1.c_nianhao_id = BIOG_ADDR_DATA.c_ly_nh_code
                )
                LEFT JOIN YEAR_RANGE_CODES ON BIOG_ADDR_DATA.c_fy_range = YEAR_RANGE_CODES.c_range_code
            )
            LEFT JOIN YEAR_RANGE_CODES AS YEAR_RANGE_CODES_1 ON BIOG_ADDR_DATA.c_ly_range = YEAR_RANGE_CODES_1.c_range_code
        )
        LEFT JOIN GANZHI_CODES ON BIOG_ADDR_DATA.c_fy_day_gz = GANZHI_CODES.c_ganzhi_code
    )
    LEFT JOIN GANZHI_CODES AS GANZHI_CODES_1 ON BIOG_ADDR_DATA.c_ly_day_gz = GANZHI_CODES_1.c_ganzhi_code;
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
    BIOG_INST_DATA.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    BIOG_INST_DATA.c_inst_name_code,
    BIOG_INST_DATA.c_inst_code,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_hz,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_py,
    BIOG_INST_DATA.c_bi_role_code,
    BIOG_INST_CODES.c_bi_role_desc,
    BIOG_INST_CODES.c_bi_role_chn,
    BIOG_INST_DATA.c_bi_begin_year,
    BIOG_INST_DATA.c_bi_by_nh_code,
    NIAN_HAO.c_nianhao_chn AS c_bi_by_nh_chn,
    NIAN_HAO.c_nianhao_pin AS c_bi_by_nh_py,
    BIOG_INST_DATA.c_bi_by_nh_year,
    BIOG_INST_DATA.c_bi_by_range,
    YEAR_RANGE_CODES.c_range AS c_bi_by_range_desc,
    YEAR_RANGE_CODES.c_range_chn AS c_bi_by_range_chn,
    BIOG_INST_DATA.c_bi_end_year,
    BIOG_INST_DATA.c_bi_ey_nh_code,
    NIAN_HAO_1.c_nianhao_chn AS c_bi_ey_nh_chn,
    NIAN_HAO_1.c_nianhao_pin AS c_bi_ey_nh_py,
    BIOG_INST_DATA.c_bi_ey_nh_year,
    BIOG_INST_DATA.c_bi_ey_range,
    YEAR_RANGE_CODES_1.c_range AS c_bi_ey_range_desc,
    YEAR_RANGE_CODES_1.c_range_chn AS c_bi_ey_range_chn,
    BIOG_INST_DATA.c_source,
    TEXT_CODES.c_title_chn AS c_source_chn,
    TEXT_CODES.c_title AS c_source_py,
    BIOG_INST_DATA.c_pages,
    BIOG_INST_DATA.c_notes,
    SOCIAL_INSTITUTION_ADDR.c_inst_addr_id,
    SOCIAL_INSTITUTION_ADDR.c_inst_addr_type_code,
    SOCIAL_INSTITUTION_ADDR.inst_xcoord,
    SOCIAL_INSTITUTION_ADDR.inst_ycoord
FROM
    (
        TEXT_CODES
        RIGHT JOIN (
            (
                YEAR_RANGE_CODES
                RIGHT JOIN (
                    (
                        NIAN_HAO
                        RIGHT JOIN (
                            BIOG_INST_CODES
                            INNER JOIN (
                                (
                                    BIOG_MAIN
                                    INNER JOIN BIOG_INST_DATA ON BIOG_MAIN.c_personid = BIOG_INST_DATA.c_personid
                                )
                                INNER JOIN SOCIAL_INSTITUTION_NAME_CODES ON BIOG_INST_DATA.c_inst_name_code = SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_code
                            ) ON BIOG_INST_CODES.c_bi_role_code = BIOG_INST_DATA.c_bi_role_code
                        ) ON NIAN_HAO.c_nianhao_id = BIOG_INST_DATA.c_bi_by_nh_code
                    )
                    LEFT JOIN NIAN_HAO AS NIAN_HAO_1 ON BIOG_INST_DATA.c_bi_ey_nh_code = NIAN_HAO_1.c_nianhao_id
                ) ON YEAR_RANGE_CODES.c_range_code = BIOG_INST_DATA.c_bi_by_range
            )
            LEFT JOIN YEAR_RANGE_CODES AS YEAR_RANGE_CODES_1 ON BIOG_INST_DATA.c_bi_ey_range = YEAR_RANGE_CODES_1.c_range_code
        ) ON TEXT_CODES.c_textid = BIOG_INST_DATA.c_source
    )
    LEFT JOIN SOCIAL_INSTITUTION_ADDR ON (
        BIOG_INST_DATA.c_inst_name_code = SOCIAL_INSTITUTION_ADDR.c_inst_name_code
    )
    AND (
        BIOG_INST_DATA.c_inst_code = SOCIAL_INSTITUTION_ADDR.c_inst_code
    );
SQL
echo "Finished view View_BiogInstData."

# View_BiogSourceData: couples biographies with their textual sources and link
# metadata.
echo "Creating view View_BiogSourceData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_BiogSourceData;
CREATE VIEW View_BiogSourceData AS
SELECT
    BIOG_SOURCE_DATA.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    BIOG_SOURCE_DATA.c_textid,
    TEXT_CODES.c_title_chn,
    TEXT_CODES.c_title,
    BIOG_SOURCE_DATA.c_pages,
    TEXT_CODES.c_url_api,
    TEXT_CODES.c_url_api_coda,
    TEXT_CODES.c_url_homepage,
    BIOG_SOURCE_DATA.c_notes,
    BIOG_SOURCE_DATA.c_main_source,
    BIOG_SOURCE_DATA.c_self_bio,
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
    BIOG_TEXT_DATA.c_textid,
    TEXT_CODES.c_title,
    TEXT_CODES.c_title_chn,
    BIOG_TEXT_DATA.c_role_id,
    TEXT_ROLE_CODES.c_role_desc,
    TEXT_ROLE_CODES.c_role_desc_chn,
    BIOG_TEXT_DATA.c_year,
    BIOG_TEXT_DATA.c_source,
    TEXT_CODES_1.c_title AS c_source_title,
    TEXT_CODES_1.c_title_chn AS c_source_chn,
    BIOG_TEXT_DATA.c_pages,
    BIOG_TEXT_DATA.c_notes
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
    ENTRY_DATA.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    BIOG_MAIN.c_index_year,
    BIOG_MAIN.c_index_year_type_code,
    INDEXYEAR_TYPE_CODES.c_index_year_type_desc,
    INDEXYEAR_TYPE_CODES.c_index_year_type_hz,
    BIOG_MAIN.c_dy,
    DYNASTIES.c_dynasty,
    DYNASTIES.c_dynasty_chn,
    ENTRY_DATA.c_entry_code,
    ENTRY_CODES.c_entry_desc,
    ENTRY_CODES.c_entry_desc_chn,
    ENTRY_DATA.c_year,
    ENTRY_DATA.c_sequence,
    BIOG_MAIN.c_index_addr_id,
    ADDR_CODES_1.c_name AS c_addr_name,
    ADDR_CODES_1.c_name_chn AS c_addr_chn,
    ADDR_CODES_1.x_coord,
    ADDR_CODES_1.y_coord,
    BIOG_MAIN.c_index_addr_type_code,
    BIOG_ADDR_CODES.c_addr_desc,
    BIOG_ADDR_CODES.c_addr_desc_chn,
    ENTRY_DATA.c_exam_rank,
    ENTRY_DATA.c_kin_code,
    KINSHIP_CODES.c_kinrel_chn,
    KINSHIP_CODES.c_kinrel,
    ENTRY_DATA.c_kin_id,
    BIOG_MAIN_1.c_name AS c_kin_name,
    BIOG_MAIN_1.c_name_chn AS c_kin_name_chn,
    ENTRY_DATA.c_assoc_code,
    ASSOC_CODES.c_assoc_desc,
    ASSOC_CODES.c_assoc_desc_chn,
    ENTRY_DATA.c_assoc_id,
    BIOG_MAIN_2.c_name AS c_assoc_name,
    BIOG_MAIN_2.c_name_chn AS c_assoc_name_chn,
    ENTRY_DATA.c_age,
    ENTRY_DATA.c_nianhao_id,
    NIAN_HAO.c_nianhao_chn,
    NIAN_HAO.c_nianhao_pin,
    ENTRY_DATA.c_entry_nh_year,
    ENTRY_DATA.c_entry_range,
    YEAR_RANGE_CODES.c_range,
    YEAR_RANGE_CODES.c_range_chn,
    ENTRY_DATA.c_inst_code,
    ENTRY_DATA.c_inst_name_code,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_hz,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_py,
    ENTRY_DATA.c_exam_field,
    ENTRY_DATA.c_entry_addr_id,
    ADDR_CODES.c_name AS c_entry_addr_name,
    ADDR_CODES.c_name_chn AS c_entry_addr_chn,
    ADDR_CODES.x_coord AS c_entry_xcoord,
    ADDR_CODES.y_coord AS c_entry_ycoord,
    ENTRY_DATA.c_parental_status,
    PARENTAL_STATUS_CODES.c_parental_status_desc,
    PARENTAL_STATUS_CODES.c_parental_status_desc_chn,
    ENTRY_DATA.c_attempt_count,
    ENTRY_DATA.c_source,
    TEXT_CODES.c_title,
    TEXT_CODES.c_title_chn,
    ENTRY_DATA.c_pages,
    ENTRY_DATA.c_notes,
    ENTRY_DATA.c_posting_notes
FROM
    (
        (
            (
                ADDR_CODES AS ADDR_CODES_1
                RIGHT JOIN (
                    (
                        TEXT_CODES
                        RIGHT JOIN (
                            ADDR_CODES
                            RIGHT JOIN (
                                YEAR_RANGE_CODES
                                RIGHT JOIN (
                                    NIAN_HAO
                                    RIGHT JOIN (
                                        PARENTAL_STATUS_CODES
                                        RIGHT JOIN (
                                            KINSHIP_CODES
                                            INNER JOIN (
                                                ASSOC_CODES
                                                INNER JOIN (
                                                    (
                                                        (
                                                            BIOG_MAIN
                                                            INNER JOIN (
                                                                ENTRY_CODES
                                                                INNER JOIN ENTRY_DATA ON ENTRY_CODES.c_entry_code = ENTRY_DATA.c_entry_code
                                                            ) ON BIOG_MAIN.c_personid = ENTRY_DATA.c_personid
                                                        )
                                                        INNER JOIN BIOG_MAIN AS BIOG_MAIN_1 ON ENTRY_DATA.c_kin_id = BIOG_MAIN_1.c_personid
                                                    )
                                                    INNER JOIN BIOG_MAIN AS BIOG_MAIN_2 ON ENTRY_DATA.c_assoc_id = BIOG_MAIN_2.c_personid
                                                ) ON ASSOC_CODES.c_assoc_code = ENTRY_DATA.c_assoc_code
                                            ) ON KINSHIP_CODES.c_kincode = ENTRY_DATA.c_kin_code
                                        ) ON PARENTAL_STATUS_CODES.c_parental_status_code = ENTRY_DATA.c_parental_status
                                    ) ON NIAN_HAO.c_nianhao_id = ENTRY_DATA.c_nianhao_id
                                ) ON YEAR_RANGE_CODES.c_range_code = ENTRY_DATA.c_entry_range
                            ) ON ADDR_CODES.c_addr_id = ENTRY_DATA.c_entry_addr_id
                        ) ON TEXT_CODES.c_textid = ENTRY_DATA.c_source
                    )
                    LEFT JOIN INDEXYEAR_TYPE_CODES ON BIOG_MAIN.c_index_year_type_code = INDEXYEAR_TYPE_CODES.c_index_year_type_code
                ) ON ADDR_CODES_1.c_addr_id = BIOG_MAIN.c_index_addr_id
            )
            LEFT JOIN BIOG_ADDR_CODES ON BIOG_MAIN.c_index_addr_type_code = BIOG_ADDR_CODES.c_addr_type
        )
        INNER JOIN SOCIAL_INSTITUTION_NAME_CODES ON ENTRY_DATA.c_inst_name_code = SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_code
    )
    LEFT JOIN DYNASTIES ON BIOG_MAIN.c_dy = DYNASTIES.c_dy;
SQL
echo "Finished view View_Entry."

# View_EventAddr & View_EventData: combine event participation with address,
# reign, and text info.
echo "Creating view View_EventAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_EventAddr;
CREATE VIEW View_EventAddr AS
SELECT
    EVENTS_ADDR.c_personid,
    BIOG_MAIN.c_name AS c_person_name,
    BIOG_MAIN.c_name_chn AS c_person_chn,
    EVENTS_ADDR.c_event_code,
    EVENT_CODES.c_event_name_chn,
    EVENT_CODES.c_event_name,
    EVENTS_ADDR.c_sequence,
    EVENTS_ADDR.c_addr_id,
    ADDR_CODES.c_name AS c_event_addr_name,
    ADDR_CODES.c_name_chn AS c_event_addr_chn,
    ADDR_CODES.x_coord AS c_event_xcoord,
    ADDR_CODES.y_coord AS c_event_ycoord,
    EVENTS_ADDR.c_year,
    EVENTS_ADDR.c_nh_code,
    NIAN_HAO.c_nianhao_chn,
    NIAN_HAO.c_nianhao_pin,
    EVENTS_ADDR.c_nh_year,
    EVENTS_ADDR.c_yr_range,
    YEAR_RANGE_CODES.c_range,
    YEAR_RANGE_CODES.c_range_chn,
    EVENTS_ADDR.c_intercalary,
    EVENTS_ADDR.c_month,
    EVENTS_ADDR.c_day,
    EVENTS_ADDR.c_day_ganzhi,
    GANZHI_CODES.c_ganzhi_chn AS c_event_day_gz_chn,
    GANZHI_CODES.c_ganzhi_py AS c_event_day_gz_py
FROM
    NIAN_HAO
    RIGHT JOIN (
        YEAR_RANGE_CODES
        RIGHT JOIN (
            GANZHI_CODES
            RIGHT JOIN (
                (
                    (
                        ADDR_CODES
                        INNER JOIN EVENTS_ADDR ON ADDR_CODES.c_addr_id = EVENTS_ADDR.c_addr_id
                    )
                    INNER JOIN BIOG_MAIN ON EVENTS_ADDR.c_personid = BIOG_MAIN.c_personid
                )
                INNER JOIN EVENT_CODES ON EVENTS_ADDR.c_event_code = EVENT_CODES.c_event_code
            ) ON GANZHI_CODES.c_ganzhi_code = EVENTS_ADDR.c_day_ganzhi
        ) ON YEAR_RANGE_CODES.c_range_code = EVENTS_ADDR.c_yr_range
    ) ON NIAN_HAO.c_nianhao_id = EVENTS_ADDR.c_nh_code;
SQL
echo "Finished view View_EventAddr."

# See comment above.
echo "Creating view View_EventData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_EventData;
CREATE VIEW View_EventData AS
SELECT
    EVENTS_DATA.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    EVENTS_DATA.c_sequence,
    EVENTS_DATA.c_event_code,
    EVENT_CODES.c_event_name_chn,
    EVENT_CODES.c_event_name,
    EVENTS_DATA.c_role,
    EVENTS_DATA.c_year,
    EVENTS_DATA.c_nh_code,
    NIAN_HAO.c_nianhao_chn,
    NIAN_HAO.c_nianhao_pin,
    EVENTS_DATA.c_nh_year,
    EVENTS_DATA.c_yr_range,
    YEAR_RANGE_CODES.c_range,
    YEAR_RANGE_CODES.c_range_chn,
    EVENTS_DATA.c_intercalary,
    EVENTS_DATA.c_month,
    EVENTS_DATA.c_day,
    EVENTS_DATA.c_day_ganzhi,
    GANZHI_CODES.c_ganzhi_chn AS c_event_day_gz_chn,
    GANZHI_CODES.c_ganzhi_py AS c_event_day_gz_py,
    EVENTS_DATA.c_source,
    TEXT_CODES.c_title AS c_source_title,
    TEXT_CODES.c_title_chn AS c_source_chn,
    EVENTS_DATA.c_pages,
    EVENTS_DATA.c_notes,
    EVENTS_DATA.c_person_text_title,
    EVENTS_DATA.c_person_text_pages,
    EVENTS_DATA.c_person_text_notes
FROM
    TEXT_CODES
    RIGHT JOIN (
        YEAR_RANGE_CODES
        RIGHT JOIN (
            GANZHI_CODES
            RIGHT JOIN (
                NIAN_HAO
                RIGHT JOIN (
                    EVENT_CODES
                    INNER JOIN (
                        BIOG_MAIN
                        INNER JOIN EVENTS_DATA ON BIOG_MAIN.c_personid = EVENTS_DATA.c_personid
                    ) ON EVENT_CODES.c_event_code = EVENTS_DATA.c_event_code
                ) ON NIAN_HAO.c_nianhao_id = EVENTS_DATA.c_nh_code
            ) ON GANZHI_CODES.c_ganzhi_code = EVENTS_DATA.c_day_ganzhi
        ) ON YEAR_RANGE_CODES.c_range_code = EVENTS_DATA.c_yr_range
    ) ON TEXT_CODES.c_textid = EVENTS_DATA.c_source;
SQL
echo "Finished view View_EventData."

# View_KinAddr: exposes kin relations with address and citation data.
echo "Creating view View_KinAddr..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_KinAddr;
CREATE VIEW View_KinAddr AS
SELECT
    KIN_DATA.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    KIN_DATA.c_kin_id,
    BIOG_MAIN_1.c_name AS c_kin_name,
    BIOG_MAIN_1.c_name_chn AS c_kin_chn,
    KIN_DATA.c_kin_code,
    KINSHIP_CODES.c_kinrel,
    KINSHIP_CODES.c_kinrel_chn,
    KIN_DATA.c_kin_addr_id,
    ADDR_CODES.c_name AS c_addr_name,
    ADDR_CODES.c_name_chn AS c_addr_chn,
    KIN_DATA.c_source,
    TEXT_CODES.c_title,
    TEXT_CODES.c_title_chn,
    KIN_DATA.c_pages,
    KIN_DATA.c_notes,
    KIN_DATA.c_sequence
FROM
    (
        ADDR_CODES
        RIGHT JOIN KIN_DATA ON ADDR_CODES.c_addr_id = KIN_DATA.c_kin_addr_id
    )
    LEFT JOIN (
        TEXT_CODES
        RIGHT JOIN (
            (
                BIOG_MAIN
                INNER JOIN KIN_DATA AS KIN_DATA_1 ON BIOG_MAIN.c_personid = KIN_DATA_1.c_personid
            )
            LEFT JOIN BIOG_MAIN AS BIOG_MAIN_1 ON KIN_DATA_1.c_kin_id = BIOG_MAIN_1.c_personid
        ) ON TEXT_CODES.c_textid = KIN_DATA_1.c_source
    ) ON (
        KIN_DATA.c_personid = KIN_DATA_1.c_personid
    )
    AND (
        KIN_DATA.c_sequence = KIN_DATA_1.c_sequence
    )
    LEFT JOIN KINSHIP_CODES ON KIN_DATA.c_kin_code = KINSHIP_CODES.c_kincode;
SQL
echo "Finished view View_KinAddr."

# View_People: produces a denormalized “person dossier” with birth/death reign
# data, ethnicity, household, choronym, and textual references.
echo "Creating view View_People..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_People;
CREATE VIEW View_People AS
SELECT
    BIOG_MAIN.c_personid,
    BIOG_MAIN.c_name,
    BIOG_MAIN.c_name_chn,
    BIOG_MAIN.c_index_year,
    BIOG_MAIN.c_index_year_type_code,
    INDEXYEAR_TYPE_CODES.c_index_year_type_desc,
    INDEXYEAR_TYPE_CODES.c_index_year_type_hz,
    BIOG_MAIN.c_index_year_source_id,
    TEXT_CODES.c_title AS c_source_title,
    TEXT_CODES.c_title_chn AS c_source_chn,
    BIOG_MAIN.c_female,
    BIOG_MAIN.c_index_addr_id,
    ADDR_CODES.c_name AS c_index_addr_name,
    ADDR_CODES.c_name_chn AS c_index_addr_chn,
    ADDR_CODES.x_coord AS c_index_addr_x_coord,
    ADDR_CODES.y_coord AS c_index_addr_y_coord,
    BIOG_MAIN.c_index_addr_type_code,
    BIOG_ADDR_CODES.c_addr_desc,
    BIOG_ADDR_CODES.c_addr_desc_chn,
    BIOG_MAIN.c_ethnicity_code,
    ETHNICITY_TRIBE_CODES.c_ethnicity_desc,
    ETHNICITY_TRIBE_CODES.c_ethnicity_desc_chn,
    BIOG_MAIN.c_household_status_code,
    HOUSEHOLD_STATUS_CODES.c_household_status_desc,
    HOUSEHOLD_STATUS_CODES.c_household_status_desc_chn,
    BIOG_MAIN.c_tribe,
    BIOG_MAIN.c_birthyear,
    BIOG_MAIN.c_by_nh_code,
    NIAN_HAO.c_nianhao_chn AS c_by_nh_chn,
    NIAN_HAO.c_nianhao_pin AS c_by_nh_py,
    BIOG_MAIN.c_by_nh_year,
    BIOG_MAIN.c_by_range,
    YEAR_RANGE_CODES.c_range AS c_by_range_desc,
    YEAR_RANGE_CODES.c_range_chn AS c_by_range_chn,
    BIOG_MAIN.c_deathyear,
    BIOG_MAIN.c_dy_nh_code,
    NIAN_HAO_1.c_nianhao_chn AS c_dy_nh_chn,
    NIAN_HAO_1.c_nianhao_pin AS c_dy_nh_py,
    BIOG_MAIN.c_dy_nh_year,
    BIOG_MAIN.c_dy_range,
    YEAR_RANGE_CODES_1.c_range AS c_dy_range_desc,
    YEAR_RANGE_CODES_1.c_range_chn AS c_dy_range_chn,
    BIOG_MAIN.c_death_age,
    BIOG_MAIN.c_death_age_range,
    YEAR_RANGE_CODES_2.c_range AS c_death_age_range_desc,
    YEAR_RANGE_CODES_2.c_range_chn AS c_death_age_range_chn,
    BIOG_MAIN.c_fl_earliest_year,
    BIOG_MAIN.c_fl_ey_nh_code,
    NIAN_HAO_2.c_nianhao_chn AS c_fl_ey_nh_chn,
    NIAN_HAO_2.c_nianhao_pin AS c_fl_ey_nh_py,
    BIOG_MAIN.c_fl_ey_nh_year,
    BIOG_MAIN.c_fl_ey_range,
    BIOG_MAIN.c_fl_latest_year,
    BIOG_MAIN.c_fl_ly_nh_code,
    NIAN_HAO_3.c_nianhao_chn AS c_fl_ly_nh_chn,
    NIAN_HAO_3.c_nianhao_pin AS c_fl_ly_nh_py,
    BIOG_MAIN.c_fl_ly_nh_year,
    BIOG_MAIN.c_fl_ly_range,
    BIOG_MAIN.c_surname,
    BIOG_MAIN.c_surname_chn,
    BIOG_MAIN.c_mingzi,
    BIOG_MAIN.c_mingzi_chn,
    BIOG_MAIN.c_dy,
    DYNASTIES.c_dynasty,
    DYNASTIES.c_dynasty_chn,
    BIOG_MAIN.c_choronym_code,
    CHORONYM_CODES.c_choronym_desc,
    CHORONYM_CODES.c_choronym_desc_chn,
    BIOG_MAIN.c_notes,
    BIOG_MAIN.c_by_intercalary,
    BIOG_MAIN.c_dy_intercalary,
    BIOG_MAIN.c_by_month,
    BIOG_MAIN.c_dy_month,
    BIOG_MAIN.c_by_day,
    BIOG_MAIN.c_dy_day,
    BIOG_MAIN.c_by_day_gz,
    GANZHI_CODES.c_ganzhi_chn AS c_by_day_gz_chn,
    GANZHI_CODES.c_ganzhi_py AS c_by_day_gz_py,
    BIOG_MAIN.c_dy_day_gz,
    GANZHI_CODES_1.c_ganzhi_chn AS c_dy_day_gz_chn,
    GANZHI_CODES_1.c_ganzhi_py AS c_dy_day_gz_py,
    BIOG_MAIN.c_surname_proper,
    BIOG_MAIN.c_mingzi_proper,
    BIOG_MAIN.c_name_proper,
    BIOG_MAIN.c_surname_rm,
    BIOG_MAIN.c_mingzi_rm,
    BIOG_MAIN.c_name_rm,
    BIOG_MAIN.c_created_by,
    BIOG_MAIN.c_created_date,
    BIOG_MAIN.c_modified_by,
    BIOG_MAIN.c_modified_date,
    BIOG_MAIN.c_self_bio
FROM
    (
        GANZHI_CODES
        RIGHT JOIN (
            GANZHI_CODES AS GANZHI_CODES_1
            RIGHT JOIN (
                DYNASTIES
                RIGHT JOIN (
                    CHORONYM_CODES
                    RIGHT JOIN (
                        (
                            (
                                (
                                    YEAR_RANGE_CODES AS YEAR_RANGE_CODES_1
                                    RIGHT JOIN (
                                        YEAR_RANGE_CODES
                                        RIGHT JOIN (
                                            (
                                                NIAN_HAO
                                                RIGHT JOIN (
                                                    HOUSEHOLD_STATUS_CODES
                                                    RIGHT JOIN (
                                                        ETHNICITY_TRIBE_CODES
                                                        RIGHT JOIN (
                                                            (
                                                                (
                                                                    BIOG_MAIN
                                                                    LEFT JOIN ADDR_CODES ON BIOG_MAIN.c_index_addr_id = ADDR_CODES.c_addr_id
                                                                )
                                                                LEFT JOIN INDEXYEAR_TYPE_CODES ON BIOG_MAIN.c_index_year_type_code = INDEXYEAR_TYPE_CODES.c_index_year_type_code
                                                            )
                                                            LEFT JOIN TEXT_CODES ON BIOG_MAIN.c_index_year_source_id = TEXT_CODES.c_textid
                                                        ) ON ETHNICITY_TRIBE_CODES.c_ethnicity_code = BIOG_MAIN.c_ethnicity_code
                                                    ) ON HOUSEHOLD_STATUS_CODES.c_household_status_code = BIOG_MAIN.c_household_status_code
                                                ) ON NIAN_HAO.c_nianhao_id = BIOG_MAIN.c_by_nh_code
                                            )
                                            LEFT JOIN NIAN_HAO AS NIAN_HAO_1 ON BIOG_MAIN.c_dy_nh_code = NIAN_HAO_1.c_nianhao_id
                                        ) ON YEAR_RANGE_CODES.c_range_code = BIOG_MAIN.c_by_range
                                    ) ON YEAR_RANGE_CODES_1.c_range_code = BIOG_MAIN.c_dy_range
                                )
                                LEFT JOIN YEAR_RANGE_CODES AS YEAR_RANGE_CODES_2 ON BIOG_MAIN.c_death_age_range = YEAR_RANGE_CODES_2.c_range_code
                            )
                            LEFT JOIN NIAN_HAO AS NIAN_HAO_2 ON BIOG_MAIN.c_fl_ey_nh_code = NIAN_HAO_2.c_nianhao_id
                        )
                        LEFT JOIN NIAN_HAO AS NIAN_HAO_3 ON BIOG_MAIN.c_fl_ly_nh_code = NIAN_HAO_3.c_nianhao_id
                    ) ON CHORONYM_CODES.c_choronym_code = BIOG_MAIN.c_choronym_code
                ) ON DYNASTIES.c_dy = BIOG_MAIN.c_dy
            ) ON GANZHI_CODES.c_ganzhi_code = BIOG_MAIN.c_by_day_gz
        ) ON GANZHI_CODES_1.c_ganzhi_code = BIOG_MAIN.c_dy_day_gz
    )
    LEFT JOIN BIOG_ADDR_CODES ON BIOG_MAIN.c_index_addr_type_code = BIOG_ADDR_CODES.c_addr_type;
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
    POSSESSION_DATA.c_personid,
    POSSESSION_DATA.c_possession_record_id,
    POSSESSION_DATA.c_sequence,
    POSSESSION_DATA.c_possession_act_code,
    POSSESSION_ACT_CODES.c_possession_act_desc,
    POSSESSION_ACT_CODES.c_possession_act_desc_chn,
    POSSESSION_DATA.c_possession_desc,
    POSSESSION_DATA.c_possession_desc_chn,
    POSSESSION_DATA.c_quantity,
    POSSESSION_DATA.c_measure_code,
    MEASURE_CODES.c_measure_desc,
    MEASURE_CODES.c_measure_desc_chn,
    POSSESSION_DATA.c_possession_yr,
    POSSESSION_DATA.c_possession_nh_code,
    NIAN_HAO.c_nianhao_chn,
    NIAN_HAO.c_nianhao_pin,
    POSSESSION_DATA.c_possession_nh_yr,
    POSSESSION_DATA.c_possession_yr_range,
    YEAR_RANGE_CODES.c_range,
    YEAR_RANGE_CODES.c_range_chn,
    POSSESSION_DATA.c_source,
    TEXT_CODES.c_title_chn,
    TEXT_CODES.c_title,
    POSSESSION_DATA.c_pages,
    POSSESSION_DATA.c_notes,
    POSSESSION_ADDR.c_addr_id
FROM
    MEASURE_CODES
    RIGHT JOIN (
        POSSESSION_ACT_CODES
        RIGHT JOIN (
            NIAN_HAO
            RIGHT JOIN (
                (
                    (
                        POSSESSION_DATA
                        LEFT JOIN YEAR_RANGE_CODES ON POSSESSION_DATA.c_possession_yr_range = YEAR_RANGE_CODES.c_range_code
                    )
                    LEFT JOIN TEXT_CODES ON POSSESSION_DATA.c_source = TEXT_CODES.c_textid
                )
                LEFT JOIN POSSESSION_ADDR ON POSSESSION_DATA.c_possession_record_id = POSSESSION_ADDR.c_possession_record_id
            ) ON NIAN_HAO.c_nianhao_id = POSSESSION_DATA.c_possession_nh_code
        ) ON POSSESSION_ACT_CODES.c_possession_act_code = POSSESSION_DATA.c_possession_act_code
    ) ON MEASURE_CODES.c_measure_code = POSSESSION_DATA.c_measure_code;
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
    POSTED_TO_OFFICE_DATA.c_personid,
    POSTED_TO_OFFICE_DATA.c_office_id,
    OFFICE_CODES.c_office_pinyin,
    OFFICE_CODES.c_office_chn,
    OFFICE_CODES.c_office_trans,
    POSTED_TO_OFFICE_DATA.c_posting_id,
    POSTED_TO_OFFICE_DATA.c_sequence,
    POSTED_TO_OFFICE_DATA.c_firstyear,
    POSTED_TO_OFFICE_DATA.c_fy_nh_code,
    NIAN_HAO.c_nianhao_chn AS c_fy_nh_chn,
    NIAN_HAO.c_nianhao_pin AS c_fy_nh_py,
    POSTED_TO_OFFICE_DATA.c_fy_nh_year,
    POSTED_TO_OFFICE_DATA.c_fy_range,
    YEAR_RANGE_CODES.c_range AS c_fy_range_desc,
    YEAR_RANGE_CODES.c_range_chn AS c_fy_range_chn,
    POSTED_TO_OFFICE_DATA.c_lastyear,
    POSTED_TO_OFFICE_DATA.c_ly_nh_code,
    NIAN_HAO_1.c_nianhao_chn AS c_ly_nh_chn,
    NIAN_HAO_1.c_nianhao_pin AS c_ly_nh_py,
    POSTED_TO_OFFICE_DATA.c_ly_nh_year,
    POSTED_TO_OFFICE_DATA.c_ly_range,
    YEAR_RANGE_CODES_1.c_range AS c_ly_range_desc,
    YEAR_RANGE_CODES_1.c_range_chn AS c_ly_range_chn,
    POSTED_TO_OFFICE_DATA.c_appt_code,
    APPOINTMENT_CODES.c_appt_desc_chn,
    APPOINTMENT_CODES.c_appt_desc,
    POSTED_TO_OFFICE_DATA.c_assume_office_code,
    ASSUME_OFFICE_CODES.c_assume_office_desc_chn,
    ASSUME_OFFICE_CODES.c_assume_office_desc,
    POSTED_TO_OFFICE_DATA.c_inst_code,
    POSTED_TO_OFFICE_DATA.c_inst_name_code,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_hz,
    SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_py,
    POSTED_TO_OFFICE_DATA.c_source,
    TEXT_CODES.c_title_chn,
    TEXT_CODES.c_title,
    POSTED_TO_OFFICE_DATA.c_pages,
    POSTED_TO_OFFICE_DATA.c_notes,
    POSTED_TO_OFFICE_DATA.c_office_category_id,
    OFFICE_CATEGORIES.c_category_desc,
    OFFICE_CATEGORIES.c_category_desc_chn,
    POSTED_TO_OFFICE_DATA.c_fy_intercalary,
    POSTED_TO_OFFICE_DATA.c_fy_month,
    POSTED_TO_OFFICE_DATA.c_ly_intercalary,
    POSTED_TO_OFFICE_DATA.c_ly_month,
    POSTED_TO_OFFICE_DATA.c_fy_day,
    POSTED_TO_OFFICE_DATA.c_ly_day,
    POSTED_TO_OFFICE_DATA.c_fy_day_gz,
    GANZHI_CODES.c_ganzhi_chn AS c_fy_day_gz_chn,
    GANZHI_CODES.c_ganzhi_py AS c_fy_day_gz_py,
    POSTED_TO_OFFICE_DATA.c_ly_day_gz,
    GANZHI_CODES_1.c_ganzhi_chn AS c_ly_day_gz_chn,
    GANZHI_CODES_1.c_ganzhi_py AS c_ly_day_gz_py,
    POSTED_TO_OFFICE_DATA.c_dy,
    DYNASTIES.c_dynasty,
    DYNASTIES.c_dynasty_chn
FROM
    BIOG_MAIN
    INNER JOIN (
        SOCIAL_INSTITUTION_NAME_CODES
        INNER JOIN (
            (
                (
                    (
                        OFFICE_CATEGORIES
                        RIGHT JOIN (
                            (
                                GANZHI_CODES
                                RIGHT JOIN (
                                    TEXT_CODES
                                    RIGHT JOIN (
                                        ASSUME_OFFICE_CODES
                                        RIGHT JOIN (
                                            APPOINTMENT_CODES
                                            RIGHT JOIN (
                                                (
                                                    (
                                                        OFFICE_CODES
                                                        INNER JOIN POSTED_TO_OFFICE_DATA ON OFFICE_CODES.c_office_id = POSTED_TO_OFFICE_DATA.c_office_id
                                                    )
                                                    LEFT JOIN NIAN_HAO AS NIAN_HAO_1 ON POSTED_TO_OFFICE_DATA.c_ly_nh_code = NIAN_HAO_1.c_nianhao_id
                                                )
                                                LEFT JOIN YEAR_RANGE_CODES AS YEAR_RANGE_CODES_1 ON POSTED_TO_OFFICE_DATA.c_ly_range = YEAR_RANGE_CODES_1.c_range_code
                                            ) ON APPOINTMENT_CODES.c_appt_code = POSTED_TO_OFFICE_DATA.c_appt_code
                                        ) ON ASSUME_OFFICE_CODES.c_assume_office_code = POSTED_TO_OFFICE_DATA.c_assume_office_code
                                    ) ON TEXT_CODES.c_textid = POSTED_TO_OFFICE_DATA.c_source
                                ) ON GANZHI_CODES.c_ganzhi_code = POSTED_TO_OFFICE_DATA.c_fy_day_gz
                            )
                            LEFT JOIN GANZHI_CODES AS GANZHI_CODES_1 ON POSTED_TO_OFFICE_DATA.c_ly_day_gz = GANZHI_CODES_1.c_ganzhi_code
                        ) ON OFFICE_CATEGORIES.c_office_category_id = POSTED_TO_OFFICE_DATA.c_office_category_id
                    )
                    INNER JOIN DYNASTIES ON POSTED_TO_OFFICE_DATA.c_dy = DYNASTIES.c_dy
                )
                INNER JOIN YEAR_RANGE_CODES ON POSTED_TO_OFFICE_DATA.c_fy_range = YEAR_RANGE_CODES.c_range_code
            )
            INNER JOIN NIAN_HAO ON POSTED_TO_OFFICE_DATA.c_fy_nh_code = NIAN_HAO.c_nianhao_id
        ) ON SOCIAL_INSTITUTION_NAME_CODES.c_inst_name_code = POSTED_TO_OFFICE_DATA.c_inst_name_code
    ) ON BIOG_MAIN.c_personid = POSTED_TO_OFFICE_DATA.c_personid;
SQL
echo "Finished view View_PostingOffice."

# View_StatusData: assembles status changes with reign spans and document
# metadata.
echo "Creating view View_StatusData..."
sqlite3 "$DB_PATH" <<'SQL'
DROP VIEW IF EXISTS View_StatusData;
CREATE VIEW View_StatusData AS
SELECT
    STATUS_DATA.c_personid,
    STATUS_DATA.c_sequence,
    STATUS_DATA.c_status_code,
    STATUS_CODES.c_status_desc,
    STATUS_CODES.c_status_desc_chn,
    STATUS_DATA.c_firstyear,
    STATUS_DATA.c_fy_nh_code,
    NIAN_HAO.c_nianhao_chn AS c_fy_nh_chn,
    NIAN_HAO.c_nianhao_pin AS c_fy_nh_py,
    STATUS_DATA.c_fy_nh_year,
    STATUS_DATA.c_fy_range,
    YEAR_RANGE_CODES_1.c_range AS c_fy_range_desc,
    YEAR_RANGE_CODES_1.c_range_chn AS c_fy_range_chn,
    STATUS_DATA.c_lastyear,
    STATUS_DATA.c_ly_nh_code,
    NIAN_HAO_1.c_nianhao_chn AS c_ly_nh_chn,
    NIAN_HAO_1.c_nianhao_pin AS c_ly_nh_py,
    STATUS_DATA.c_ly_nh_year,
    STATUS_DATA.c_ly_range,
    YEAR_RANGE_CODES_1.c_range AS c_ly_range_desc,
    YEAR_RANGE_CODES_1.c_range_chn AS c_ly_range_chn,
    STATUS_DATA.c_supplement,
    STATUS_DATA.c_source,
    TEXT_CODES.c_title_chn,
    TEXT_CODES.c_title,
    STATUS_DATA.c_pages,
    STATUS_DATA.c_notes
FROM
    STATUS_CODES
    INNER JOIN (
        NIAN_HAO
        RIGHT JOIN (
            (
                (
                    (
                        STATUS_DATA
                        LEFT JOIN YEAR_RANGE_CODES ON STATUS_DATA.c_fy_range = YEAR_RANGE_CODES.c_range_code
                    )
                    LEFT JOIN TEXT_CODES ON STATUS_DATA.c_source = TEXT_CODES.c_textid
                )
                LEFT JOIN NIAN_HAO AS NIAN_HAO_1 ON STATUS_DATA.c_ly_nh_code = NIAN_HAO_1.c_nianhao_id
            )
            LEFT JOIN YEAR_RANGE_CODES AS YEAR_RANGE_CODES_1 ON STATUS_DATA.c_ly_range = YEAR_RANGE_CODES_1.c_range_code
        ) ON NIAN_HAO.c_nianhao_id = STATUS_DATA.c_fy_nh_code
    ) ON STATUS_CODES.c_status_code = STATUS_DATA.c_status_code;
SQL
echo "Finished view View_StatusData."

echo "Created CBDB views in '$DB_PATH'."
