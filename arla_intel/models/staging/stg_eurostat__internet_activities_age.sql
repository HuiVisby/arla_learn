 with source as (
      select * from {{ source('raw_ingest', 'eurostat_internet_activities_age') }}
  ),

 cleaned as (
      select
          country_code,
          age_group,
          case age_group
              when 'Y16_24' then '16-24'
              when 'Y25_34' then '25-34'
              when 'Y35_44' then '35-44'
              when 'Y45_54' then '45-54'
              when 'Y55_64' then '55-64'
              when 'Y65_74' then '65-74'
          end                         as age_label,
          activity,
          year,
          pct_individuals,
      from source
      where pct_individuals is not null
  )

select * from cleaned


