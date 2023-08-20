create or replace table STG_D_COUNTRY_LU
(
id NUMBER,
country_desc VARCHAR(256),
PRIMARY KEY (id)
)
;

create or replace table STG_D_REGION_LU
(
id NUMBER,
country_id NUMBER,
region_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (country_id) references STG_D_COUNTRY_LU(id) 
)
;

create or replace table STG_D_STORE_B
(
id NUMBER,
region_id NUMBER,
store_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (region_id) references STG_D_REGION_LU(id) 
)
;

create or replace table STG_D_CATEGORY_LU
(
id NUMBER,
category_desc VARCHAR(1024),
PRIMARY KEY (id)
);

create or replace table STG_D_SUB_CATEGORY_LU
(
id NUMBER,
category_id NUMBER,
subcategory_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (category_id) references STG_D_CATEGORY_LU(id) 
);

create or replace table STG_D_PRODUCT_B
(
id NUMBER,
subcategory_id NUMBER,
product_desc VARCHAR(256),
PRIMARY KEY (id),
FOREIGN KEY (subcategory_id) references STG_D_SUB_CATEGORY_LU(id)
);

create or replace table STG_D_CUSTOMER_B
(
id NUMBER,
customer_first_name VARCHAR(256),
customer_middle_name VARCHAR(256),
customer_last_name VARCHAR(256),
customer_address VARCHAR(256) ,
primary key (id)
);

create or replace table STG_F_TXN_B
(
id NUMBER,
store_id NUMBER NOT NULL,
product_id NUMBER NOT NULL,
customer_id NUMBER,
transaction_time TIMESTAMP,
quantity NUMBER,
amount NUMBER(20,2),
discount NUMBER(20,2),
primary key (id),
FOREIGN KEY (store_id) references STG_D_STORE_B(id),
FOREIGN KEY (product_id) references STG_D_PRODUCT_B(id),
FOREIGN KEY (customer_id) references STG_D_CUSTOMER_B(id)
);
