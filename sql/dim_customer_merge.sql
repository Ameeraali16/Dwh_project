-- Modified approach to handle duplicates in source data
MERGE INTO ECOMMERCE_DB.STAR_SCHEMA.DIM_CUSTOMER tgt
USING (
    -- Deduplicate the source data by taking the latest record for each customer_id
    SELECT 
        CUSTOMER_ID,
        NAME,
        EMAIL,
        PHONE_NO,
        ADDRESS,
        CITY,
        LOYALTY_TIER,
        POINTS_EARNED,
        IS_ACTIVE
    FROM (
        SELECT 
            c.CUSTOMER_ID,
            c.NAME,
            c.EMAIL,
            c.PHONE AS PHONE_NO,
            c.ADDRESS,
            c.CITY,
            ld.LOYALTY_TIER,
            ld.POINTS_EARNED,
            ld.IS_ACTIVE,
            -- Add row number to pick only one record per customer_id
            ROW_NUMBER() OVER (PARTITION BY c.CUSTOMER_ID ORDER BY c.CUSTOMER_ID) AS rn
        FROM ECOMMERCE_DB.ECOMMERCE_SCHEMA.CUSTOMERS c
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY LOYALTY_ID DESC) AS rn
                FROM ECOMMERCE_DB.ECOMMERCE_SCHEMA.CUSTOMER_LOYALTY
            )
            WHERE rn = 1
        ) ld ON c.CUSTOMER_ID = ld.CUSTOMER_ID
        WHERE c.CUSTOMER_ID IS NOT NULL
    ) deduplicated
    WHERE rn = 1  -- Take only one record per customer_id
) src ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID)

WHEN MATCHED THEN
    UPDATE SET
        tgt.NAME = src.NAME,
        tgt.EMAIL = src.EMAIL,
        tgt.PHONE_NO = src.PHONE_NO,
        tgt.ADDRESS = src.ADDRESS,
        tgt.CITY = src.CITY,
        tgt.LOYALTY_TIER = src.LOYALTY_TIER,
        tgt.POINTS_EARNED = src.POINTS_EARNED,
        tgt.IS_ACTIVE = src.IS_ACTIVE

WHEN NOT MATCHED THEN
    INSERT (
        CUSTOMER_ID, NAME, EMAIL, PHONE_NO, ADDRESS, CITY, LOYALTY_TIER, POINTS_EARNED, IS_ACTIVE
    )
    VALUES (
        src.CUSTOMER_ID, src.NAME, src.EMAIL, src.PHONE_NO, src.ADDRESS, src.CITY, src.LOYALTY_TIER, src.POINTS_EARNED, src.IS_ACTIVE
    );