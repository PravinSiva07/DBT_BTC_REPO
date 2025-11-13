{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append'
    )
}}

with flatteed_result as (
select 
src.HASH_KEY,
src.block_number,
src.BLOCK_TIMESTAMP,
src.is_coinbase,
f.value:address::varchar as output_address,
f.value:value::float as output_value

from 
{{ ref('stg_btc') }} src ,
lateral flatten(input => outputs) f
where output_address is not null

{% if is_incremental() %}
  and src.BLOCK_TIMESTAMP >=  (select max(BLOCK_TIMESTAMP) from {{ this }})
{% endif %}
)
select hash_key,
block_number,
block_timestamp,
is_coinbase,
output_address,
output_value
from flatteed_result