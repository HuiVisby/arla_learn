 {{
      config(materialized='table')
  }}

  with weekly as (
      select
          week_start,
          year,
          month,
          keyword,
          keyword_category,
          search_interest
      from {{ ref('stg_google_trends__dairy_weekly') }}
  ),

  regional as (
      select
          region,
          keyword,
          search_interest as regional_interest
      from {{ ref('stg_google_trends__dairy_regional') }}
  )

  select
      w.week_start,
      w.year,
      w.month,
      w.keyword,
      w.keyword_category,
      w.search_interest
  from weekly w
  order by w.keyword, w.week_start

