from scripts.common.connect import connection

conn= connection()
cursor = conn.cursor()
## Product

# When new record is added to the source
cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_product_b(product_id, product_desc, subcategory_key)
    SELECT  id, product_desc, subcategory_key 
    FROM etl.stg.stg_d_product_b prod
    JOIN etl.tgt.tgt_d_sub_category_lu subcat
        ON prod.subcategory_id = subcat.subcategory_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_product_b dest_prod
        WHERE dest_prod.product_id = prod.id
    );
    """
)

# When a record, that was once removed, is added to the source again
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_product_b dest
    SET active_flag = TRUE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        select 1
        FROM etl.stg.stg_d_product_b source
        WHERE dest.product_id = source.id AND dest.active_flag = FALSE
    );
    """
)

# When a record is removed from the source
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_product_b dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_product_b source
        WHERE source.id = dest.product_id
    );
    """
)

# Minor  change - when the name of a product is changed
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_product_b dest
    SET product_desc = source.product_desc
    FROM etl.stg.stg_d_product_b source
    WHERE dest.product_id = source.id
        AND dest.product_desc != source.product_desc;
    """
)

# Major change - when the category of a record is changed
cursor.execute("BEGIN TRANSACTION")
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_product_b dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT prod.id, product_desc, subcategory_key
            FROM etl.stg.stg_d_product_b prod
            JOIN etl.tgt.tgt_d_sub_category_lu subcat
                ON prod.subcategory_id = subcat.subcategory_id
        ) source
        WHERE dest.product_id = source.id AND dest.subcategory_key != source.subcategory_key
    );
    """
)

cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_product_b(product_id, product_desc, subcategory_key)
    SELECT  id, product_desc, subcategory_key
    FROM (
        SELECT prod.id, product_desc, subcategory_key
        FROM etl.stg.stg_d_product_b prod
        JOIN etl.tgt.tgt_d_sub_category_lu subcat
            ON prod.subcategory_id = subcat.subcategory_id
    ) source
    WHERE EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_product_b dest
        WHERE dest.product_id = source.id AND dest.subcategory_key != source.subcategory_key
    );
    """
)
cursor.execute("COMMIT")
