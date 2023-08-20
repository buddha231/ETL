-- # Load the fact base table
    INSERT INTO ETL.TGT.TGT_F_TXN_B(
        TXN_ID, 
        TRANSACTION_TIME, 
        QUANTITY, 
        DISCOUNT, 
        STORE_KEY, 
        CUSTOMER_KEY,
        PRODUCT_KEY
    )
    SELECT
        sales.id,
        DATE(transaction_time),
        quantity,
        amount,
        discount
        store_key,
        customer_key,
        product_key
    FROM stg.stg_f_txn_b sales
    JOIN tgt.tgt_d_store_b store
        ON sales.store_id = store.store_id
    JOIN tgt.tgt_d_product_b product
        ON sales.product_id = product.product_id
    LEFT JOIN tgt.tgt_d_customer_b customer
        ON sales.customer_id = customer.customer_id;