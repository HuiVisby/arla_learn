  with source as (
      select * from {{ source('raw_ingest', 'scb_organic_sales') }}
  ),

  cleaned as (
      select
          Varugrupp                           as product_code,
          case Varugrupp
              when '01.1.1' then 'Cereals'
              when '01.1.2' then 'Meat'
              when '01.1.3' then 'Fish & Seafood'
              when '01.1.4' then 'Dairy & Eggs'
              when '01.1.5' then 'Oils & Fats'
              when '01.1.6' then 'Fruits & Nuts'
              when '01.1.7' then 'Vegetables'
              when '01.1.8' then 'Sugar & Confectionery'
              when '01.1.9' then 'Other Food'
              when '01.2'   then 'Non-alcoholic Drinks'
              else Varugrupp
          end                                 as product_label,
          case when Varugrupp = '01.1.4' then true else false end as is_dairy,
          cast(Tid as int64)                  as year,
          value                               as sales_sek_incl_vat,
          source
      from source
      where value is not null
  )

  select * from cleaned
