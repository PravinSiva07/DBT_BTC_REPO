{%- test valid_btc_address(model, column_name, mval) -%}

select * from {{model}}
where not(
{{column_name}} like '1%' or
{{column_name}} like '3%' or
{{column_name}} like '{{mval}}%'
)

{%- endtest  %}