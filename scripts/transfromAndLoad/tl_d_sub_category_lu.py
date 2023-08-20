from scripts.common.connect import connection

conn= connection()
cursor = conn.cursor()

## Subcategory
# When new record is added to the source
cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_sub_category_lu(subcategory_id, subcategory_desc, category_key)
    SELECT subcat.id, subcategory_desc, category_key 
    FROM etl.stg.stg_d_sub_category_lu subcat
    JOIN etl.tgt.tgt_d_category_lu cat
    ON subcat.category_id = cat.category_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_sub_category_lu dest_subcat
        WHERE dest_subcat.subcategory_id = subcat.id
    );
    """
)

# When a record, that was once removed, is added to the source again
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_sub_category_lu dest
    SET active_flag = TRUE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        select 1
        FROM etl.stg.stg_d_sub_category_lu source
        WHERE dest.subcategory_id = source.id AND dest.active_flag = FALSE
    );
    """
)

# When a record is removed from the source
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_sub_category_lu dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_sub_category_lu source
        WHERE source.id = dest.subcategory_id
    );
    """
)

# Minor  change - when the name of a subcategory is changed
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_sub_category_lu dest
    SET subcategory_desc = source.subcategory_desc
    FROM etl.stg.stg_d_sub_category_lu source
    WHERE dest.subcategory_id = source.id
        AND dest.subcategory_desc != source.subcategory_desc;
    """
)

# Major change - when the category of a record is changed
cursor.execute("BEGIN TRANSACTION")
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_sub_category_lu dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT subcat.id, subcategory_desc, category_key
            FROM etl.stg.stg_d_sub_category_lu subcat
            JOIN etl.tgt.tgt_d_category_lu cat
                ON subcat.category_id = cat.category_id
        ) source
        WHERE dest.subcategory_id = source.id AND dest.category_key != source.category_key
    );
    """
)

cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_sub_category_lu(subcategory_id, subcategory_desc, category_key)
    SELECT  id, subcategory_desc, category_key
    FROM (
        SELECT subcat.id, subcategory_desc, category_key
        FROM etl.stg.stg_d_sub_category_lu subcat
        JOIN etl.tgt.tgt_d_category_lu cat
            ON subcat.category_id = cat.category_id
    ) source
    WHERE EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_sub_category_lu dest
        WHERE dest.subcategory_id = source.id AND dest.category_key != source.category_key
    );
    """
)
cursor.execute("COMMIT")
