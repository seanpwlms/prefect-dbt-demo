with customers as (
 select * from {{ ref('stg_customer') }}
),

demogs as (
    select * from {{ ref('stg_customer_demographics') }}
),

addresses as (
    select * from {{ ref('stg_customer_address') }}
),

combined as (
    select cust.*, demog.*, addr.*
    from customers cust
    left join demogs demog
        on cust.c_current_cdemo_sk = demog.cd_demo_sk
    left join addresses addr
        on cust.c_current_addr_sk = addr.ca_address_sk
)

select * from combined