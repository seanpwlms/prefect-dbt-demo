with cte as (seelct * from {{ref('base_customer_address')}}
)

select * from cte
where ca_address_sk != 'fake_address'