MERGE INTO ECOMMERCE_DB.STAR_SCHEMA.DIM_REVIEW tgt
USING (
    WITH review_deduped AS (
        SELECT 
            REVIEW_ID,
            RATING,
            REVIEW_TEXT,
            CASE 
                WHEN REVIEW_TEXT = 'Great product, highly recommended!' THEN 0.9
                WHEN REVIEW_TEXT = 'Exceeded my expectations.' THEN 0.85
                WHEN REVIEW_TEXT = 'Perfect for my needs.' THEN 0.8
                WHEN REVIEW_TEXT = 'Would buy again.' THEN 0.75
                WHEN REVIEW_TEXT = 'Excellent customer service.' THEN 0.9
                WHEN REVIEW_TEXT = 'Average quality but good value.' THEN 0.5
                WHEN REVIEW_TEXT = 'Not worth the money.' THEN 0.2
                WHEN REVIEW_TEXT = 'Product arrived damaged.' THEN 0.1
                WHEN REVIEW_TEXT = 'Disappointing performance.' THEN 0.15
                WHEN REVIEW_TEXT = 'Shipping was very slow.' THEN 0.25
                ELSE NULL
            END AS SENTIMENT_SCORE,
            CASE 
                WHEN REVIEW_TEXT IN (
                    'Great product, highly recommended!', 
                    'Exceeded my expectations.', 
                    'Perfect for my needs.', 
                    'Would buy again.', 
                    'Excellent customer service.'
                ) THEN 'Positive'
                WHEN REVIEW_TEXT = 'Average quality but good value.' THEN 'Neutral'
                WHEN REVIEW_TEXT IN (
                    'Not worth the money.', 
                    'Product arrived damaged.', 
                    'Disappointing performance.', 
                    'Shipping was very slow.'
                ) THEN 'Negative'
                ELSE 'Unknown'
            END AS SENTIMENT_LABEL,
            ROW_NUMBER() OVER (PARTITION BY REVIEW_ID ORDER BY REVIEW_ID) AS rn
        FROM ECOMMERCE_DB.ECOMMERCE_SCHEMA.REVIEWS
    )
    SELECT 
        REVIEW_ID,
        RATING,
        REVIEW_TEXT,
        SENTIMENT_SCORE,
        SENTIMENT_LABEL
    FROM review_deduped
    WHERE rn = 1
) src
ON tgt.REVIEW_ID = src.REVIEW_ID
WHEN MATCHED THEN UPDATE SET
    RATING = src.RATING,
    REVIEW_TEXT = src.REVIEW_TEXT,
    SENTIMENT_SCORE = src.SENTIMENT_SCORE,
    SENTIMENT_LABEL = src.SENTIMENT_LABEL
WHEN NOT MATCHED THEN INSERT (
    REVIEW_ID, RATING, REVIEW_TEXT, SENTIMENT_SCORE, SENTIMENT_LABEL
) VALUES (
    src.REVIEW_ID, src.RATING, src.REVIEW_TEXT, src.SENTIMENT_SCORE, src.SENTIMENT_LABEL
);
