--  Adding new records
        INSERT INTO etl.tgt.tgt_d_country_lu(country_id, country_desc)
        SELECT SOURCE.id, SOURCE.COUNTRY_DESC 
        FROM etl.stg.stg_d_country_lu source
        WHERE NOT EXISTS (
            SELECT 1
            FROM etl.tgt.tgt_d_country_lu as dest
            WHERE dest.COUNTRY_ID = source.id
        );

-- # When a record that was once removed from the source is added again
UPDATE etl.tgt.tgt_d_country_lu as dest
        SET active_flag = TRUE,
            CREATED_TS  = CURRENT_TIMESTAMP(),
            UPDATED_TS  = TO_TIMESTAMP('9999-12-31 23:59:59.999999999')
        WHERE EXISTS (
            SELECT 1
            FROM etl.stg.stg_d_country_lu source
            WHERE source.id = dest.COUNTRY_ID  AND dest.active_flag = 'False'
        );

-- # When a record is removed from he source
        UPDATE etl.tgt.tgt_d_country_lu dest
        SET active_flag = FALSE,
            updated_TS = CURRENT_TIMESTAMP()
        WHERE NOT EXISTS (
            SELECT 1
            FROM ETL.stg.stg_d_country_lu source
            WHERE source.id = dest.country_id
        );

-- # Minor change - when the name of the country is changed
        UPDATE etl.tgt.tgt_d_country_lu dest
        SET country_desc = source.country_desc,
            updated_ts = CURRENT_TIMESTAMP()
        FROM ETL.stg.stg_d_country_lu source
        WHERE dest.country_id = source.id
            AND dest.country_desc != source.country_desc;
