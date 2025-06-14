INSERT INTO ECOMMERCE_DB.STAR_SCHEMA.DIM_DATE
SELECT DISTINCT
    DATE(ORDER_DATE) AS DATE_ID,
    EXTRACT(DAY FROM ORDER_DATE) AS DAY,
    EXTRACT(MONTH FROM ORDER_DATE) AS MONTH,
    EXTRACT(QUARTER FROM ORDER_DATE) AS QTR,
    EXTRACT(YEAR FROM ORDER_DATE) AS YEAR,
    'FY' || EXTRACT(YEAR FROM ORDER_DATE) AS FISCAL_YEAR
FROM ECOMMERCE_DB.ECOMMERCE_SCHEMA.ORDERS
WHERE DATE(ORDER_DATE) NOT IN (SELECT DATE_ID FROM ECOMMERCE_DB.STAR_SCHEMA.DIM_DATE);
