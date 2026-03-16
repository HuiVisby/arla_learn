 {{
      config(materialized='table')
  }}

  with food_retail as (
      select
          year,
          month,
          period,
          index_value,
          'Food Retail Index' as metric
      from {{ ref('stg_scb__food_retail_index') }}
  ),

  organic_dairy as (
      select
          year,
          product_label,
          sales_sek_incl_vat,
          is_dairy
      from {{ ref('stg_scb__organic_sales') }}
  ),

  organic_total as (
      select
          year,
          sum(sales_sek_incl_vat)                                     as total_organic_sales,
          sum(case when is_dairy then sales_sek_incl_vat else 0 end)  as dairy_organic_sales,
          round(
              100.0 * sum(case when is_dairy then sales_sek_incl_vat else 0 end)
              / nullif(sum(sales_sek_incl_vat), 0), 1
          )                                                            as dairy_share_pct
      from organic_dairy
      group by year
  )

  select
      f.year,
      f.month,
      f.period,
      f.index_value          as food_retail_index,
      o.total_organic_sales,
      o.dairy_organic_sales,
      o.dairy_share_pct
  from food_retail f
  left join organic_total o using (year)
  order by f.period
