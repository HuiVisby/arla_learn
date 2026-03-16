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
          case activity
              when 'I_IUSNET'  then 'Social Networks'
              when 'I_IUCHAT1' then 'Messaging & Chat'
              when 'I_IUVOD'   then 'Video on Demand'
              when 'I_IUSTV'   then 'Streaming TV'
              when 'I_IUMUSS'  then 'Music Streaming'
              when 'I_IUNW'    then 'Online News'
              when 'I_IUPCAST' then 'Podcasts'
              when 'I_IUGM'    then 'Online Gaming'
              when 'I_IUUPL'   then 'Content Creation'
          end                         as channel_name,
          year,
          pct_individuals
      from source
      where activity in (
          'I_IUSNET', 'I_IUCHAT1', 'I_IUVOD', 'I_IUSTV',
          'I_IUMUSS', 'I_IUNW', 'I_IUPCAST', 'I_IUGM', 'I_IUUPL'
      )
      and pct_individuals is not null
  )

  select * from cleaned
