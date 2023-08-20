from scripts.common.connect import connection

conn= connection()
cursor = conn.cursor()

# When new record is added to the source
cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_store_b(store_id, store_desc, region_key)
    SELECT  store.id, store_desc, region_key 
    FROM etl.stg.stg_d_store_b store
    JOIN etl.tgt.tgt_d_region_lu region
    ON store.region_id = region.region_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_store_b dest_store
        WHERE dest_store.store_id = store.id
    );
    """
)

# When a record, that was once removed, is added to the source again
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_store_b dest
    SET active_flag = TRUE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_store_b source
        WHERE dest.store_id = source.id AND dest.active_flag = FALSE
    );
    """
)

# When a record is removed from the source
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_store_b dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_store_b source
        WHERE source.id = dest.store_id
    );
    """
)

# Minor  change - when the name of a region is changed
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_store_b dest
    SET store_desc = source.store_desc
    FROM etl.stg.stg_d_store_b source
    WHERE dest.store_id = source.id
        AND dest.store_desc != source.store_desc;
    """
)

# Major change - when the category of a record is changed
cursor.execute("BEGIN TRANSACTION")
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_store_b dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT store.id, store_desc, region_key
            FROM etl.stg.stg_d_store_b store
            JOIN etl.tgt.tgt_d_region_lu region
                ON store.region_id = region.region_id
        ) source
        WHERE dest.store_id = source.id AND dest.region_key != source.region_key
    );
    """
)

cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_store_b(store_id, store_desc, region_key)
    SELECT  id, store_desc, region_key
    FROM (
        SELECT store.id, store_desc, region_key
        FROM etl.stg.stg_d_store_b store
        JOIN etl.tgt.tgt_d_region_lu region
            ON store.region_id = region.region_id
    ) source
    WHERE EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_store_b dest
        WHERE dest.store_id = source.id AND dest.region_key != source.region_key
    );
    """
)
cursor.execute("COMMIT")
