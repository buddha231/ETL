from scripts.common.connect import connection

conn= connection()
cursor = conn.cursor()
## Customer
# When new record is added to the source
cursor.execute(
    """
    INSERT INTO etl.tgt.tgt_d_customer_b(customer_id, first_name, middle_name, last_name, address)
    SELECT
        id, customer_first_name, customer_middle_name, customer_last_name, customer_address
    FROM etl.stg.stg_d_customer_b source
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.tgt.tgt_d_customer_b dest
        WHERE source.id = dest.customer_id
    );
    """
)

# When a record, that was once removed, is added to the source again
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_customer_b dest
    SET active_flag = TRUE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_customer_b source
        WHERE source.id = dest.customer_id AND dest.active_flag = 'False'
    );
    """
)

# When a record is removed from the source
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_customer_b dest
    SET active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP()
    WHERE NOT EXISTS (
        SELECT 1
        FROM etl.stg.stg_d_customer_b source
        WHERE source.id = dest.customer_id
    );
    """
)

# Minor change - when the name of the category is changed
cursor.execute(
    """
    UPDATE etl.tgt.tgt_d_customer_b dest
    SET first_name = source.customer_first_name,
        middle_name = source.customer_middle_name,
        last_name = source.customer_last_name,
        address = source.customer_address,
        updated_ts = CURRENT_TIMESTAMP()
    FROM etl.stg.stg_d_customer_b source
    WHERE dest.customer_id = source.id
        AND (dest.first_name != source.customer_first_name
            OR dest.middle_name != source.customer_middle_name
            OR dest.last_name != source.customer_last_name
            OR dest.address != source.customer_address);
    """
)
