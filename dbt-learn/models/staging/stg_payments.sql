{{ config(query_tag = 'test_payment') }}

SELECT ID
     , ORDERID AS ORDER_ID
     , PAYMENTMETHOD
     , STATUS
     , AMOUNT
     , CREATED
FROM {{ source('stripe', 'payment') }}