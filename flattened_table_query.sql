-- join both OrderTable and ActiveTable base on SERVICE_ID and REPORT_DATE to form the FlattenedTable

CREATE TABLE FlattenedTable AS
SELECT 
    t1.REPORT_DATE ,
    t1.SERVICE_ID ,
    t1.SERVICE_NAME ,
    t1.SERVICE ,
    t1.ORDER_TYPE ,
    t1.ORDER_TYPE_L2 ,
    t1.CUSTOMER_ID,
    t1.CUSTOMER_SEGMENT_FLAG,
    t1.CUSTOMER_GENDER,
    t2.ACTIVE_SUB_DATE,
    t2.SUBSCRIPTION_STATUS
FROM 
    OrderTable t1
LEFT JOIN 
    ActiveTable t2
ON 
    t1.SERVICE_ID = t2.SERVICE_ID
AND 
    t1.REPORT_DATE = t2.REPORT_DATE;

-- to get the daily, weekly or monthly frequency, aggregate the rows base on the DATE_TRUNC(REPORT_DATE)