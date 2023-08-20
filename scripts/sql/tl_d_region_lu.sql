
-- # When new record is added to the source
    INSERT INTO etl.tgt.tgt_d_region_lu(region_id, region_desc, country_key)
    SELECT region.id, region_desc, country_key 
    FROM etl.stg.stg_d_region_lu region
    JOIN etl.tgt.tgt_d_country_lu country
    ON region.country_id = country.country_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_region_lu dest_region
        WHERE dest_region.region_id = region.id
    );

-- # When a record, that was once removed, is added to the source again
    UPDATE etl.tgt.tgt_d_region_lu dest
    SET active_flag = TRUE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_region_lu source
        WHERE dest.region_id = source.id AND dest.active_flag = FALSE
    );

-- # When a record is removed from the source
    UPDATE etl.tgt.tgt_d_region_lu dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_region_lu source
        WHERE source.id = dest.region_id
    );

-- # Minor  change - when the name of a region is changed
    UPDATE etl.tgt.tgt_d_region_lu dest
    SET region_desc = source.region_desc
    FROM etl.stg.stg_d_region_lu source
    WHERE dest.region_id = source.id
        AND dest.region_desc != source.region_desc;

-- # Major change - when the category of a record is changed
BEGIN TRANSACTION;
    UPDATE etl.tgt.tgt_d_region_lu dest
    SET active_flag = FALSE,
        UPDATED_TS 	 = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT region.id, region_desc, country_key
            FROM etl.stg.stg_d_region_lu region
            JOIN etl.tgt.tgt_d_country_lu country
                ON region.country_id = country.country_id
        ) source
        WHERE dest.region_id = source.id AND dest.country_key != source.country_key 
        );

    INSERT INTO etl.tgt.tgt_d_region_lu( region_id, region_desc, country_key)
    SELECT  id , region_desc, country_key
    FROM (
        SELECT region.id, region_desc, country_key
        FROM etl.stg.stg_d_region_lu region
        JOIN etl.tgt.tgt_d_country_lu country
            ON region.country_id = country.country_id
    ) source
    WHERE EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_region_lu dest
        WHERE dest.region_id = source.id AND dest.country_key != source.country_key
    );
COMMIT