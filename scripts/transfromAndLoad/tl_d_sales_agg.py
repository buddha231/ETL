from scripts.common.connect import connection

conn= connection()
cursor = conn.cursor()

cursor.execute(
    """
    UPDATE STG.STG_F_TXN_B
    SET discount = 0
    WHERE discount IS NULL;   
    """
)        

cursor.execute("TRUNCATE TABLE tgt.TGT_F_TXN_AGG")
cursor.execute(
    """
    INSERT INTO TGT.TGT_F_TXN_AGG(
        month_name,
        store_desc, 
        total_amount,
        total_discount
    )
    SELECT
        month_name,
        store_desc,
        total_amount,
        total_discount
    FROM (
        SELECT
            MONTH(transaction_time) AS month_num,
            MONTHNAME(transaction_time) AS month_name,
            store_desc,
            SUM(amount) AS total_amount,
            SUM(discount) AS total_discount
        FROM stg.stg_f_txn_b sales
        JOIN tgt.tgt_d_store_b store
            ON sales.store_id = store.store_id
        GROUP BY month_num, month_name, store_desc
        ORDER BY month_num, store_desc
    );
    """
)