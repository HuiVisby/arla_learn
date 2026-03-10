
  with source as (
      select * from {{ source('raw_ingest', 'google_trends_dairy_weekly') }}
  ),

  cleaned as (
      select
          country_code,
          cast(date as date)          as week_start,
          extract(year from date)     as year,
          extract(month from date)    as month,
          keyword,
          case
              when keyword in ('Arla')
                  then 'Brand'
              when keyword in ('mjölk')
                  then 'Dairy'
              when keyword in ('havredryck', 'Oatly', 'Alpro')
                  then 'Plant-based'
              when keyword in ('ekologisk mjölk', 'hållbar mat', 'vegansk mat', 'laktosfri')
                  then 'Sustainability'
          end                         as keyword_category,
          search_interest
      from source
      where search_interest is not null
  )

  select * from cleaned
