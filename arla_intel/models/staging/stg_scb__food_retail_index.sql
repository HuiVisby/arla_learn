 with source as (
      select * from {{ source('raw_ingest', 'scb_food_retail_index') }}
  ),

  cleaned as (
      select
          country_code,
          period,
          cast(left(period, 4) as int64)  as year,
          cast(right(period, 2) as int64) as month,
          index_value,
          source
      from source
      where index_value is not null
  )

  select * from cleaned
  order by period

