MERGE INTO ECOMMERCE_DB.STAR_SCHEMA.DIM_PRODUCT tgt
USING (
    SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY_NAME, PRODUCT_PRICE
    FROM (
        SELECT 
            p.PRODUCT_ID,
            p.PRODUCT_NAME,
            c.CATEGORY_NAME,
            p.PRICE AS PRODUCT_PRICE,
            ROW_NUMBER() OVER (PARTITION BY p.PRODUCT_ID ORDER BY p.PRODUCT_ID) AS rn
        FROM ECOMMERCE_DB.ECOMMERCE_SCHEMA.PRODUCTS p
        LEFT JOIN ECOMMERCE_DB.ECOMMERCE_SCHEMA.CATEGORIES c 
            ON p.CATEGORY_ID = c.CATEGORY_ID
    ) sub
    WHERE rn = 1
) src
ON tgt.PRODUCT_ID = src.PRODUCT_ID
WHEN MATCHED THEN UPDATE SET
    PRODUCT_NAME = src.PRODUCT_NAME,
    CATEGORY_NAME = src.CATEGORY_NAME,
    PRODUCT_PRICE = src.PRODUCT_PRICE
WHEN NOT MATCHED THEN INSERT (
    PRODUCT_ID, PRODUCT_NAME, CATEGORY_NAME, PRODUCT_PRICE
) VALUES (
    src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY_NAME, src.PRODUCT_PRICE
);
