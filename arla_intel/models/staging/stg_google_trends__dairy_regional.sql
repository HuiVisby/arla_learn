  with source as (
      select * from {{ source('raw_ingest', 'google_trends_dairy_regional') }}
  ),

  cleaned as (
      select
          country_code,
          region,
          keyword,
          search_interest
      from source
      where search_interest > 0
  )

  select * from cleaned
