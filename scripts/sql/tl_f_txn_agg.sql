    UPDATE STG.STG_F_TXN_B
    SET discount = 0
    WHERE discount IS NULL;   

TRUNCATE TABLE tgt.TGT_F_TXN_AGG;
 INSERT INTO ETL.TGT.TGT_F_TXN_AGG(
        month_name,
        store_desc, 
        total_amount,
        total_quantity,
        total_discount
    )
    SELECT 
        month_name, 
        store_desc, 
        total_amount, 
        total_quantity,
        total_discount
    FROM (
        SELECT
            MONTH(transaction_time ) as month_num,
            MONTHNAME(transaction_time) AS month_name,
            store_desc,
            SUM(quantity) as total_quantity,
            SUM(amount) AS total_amount,
            SUM(discount) AS total_discount
        FROM ETL.stg.stg_f_txn_b sales
        JOIN etl.tgt.tgt_d_store_b store
            ON sales.store_id = store.store_id
        GROUP BY  month_num, month_name, store_desc
        Order BY month_num
    );