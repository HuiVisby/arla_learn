 {{
      config(materialized='table')
  }}

  with internet_activities as (
      select
          age_group,
          age_label,
          activity,
          year,
          avg(pct_individuals) as pct_individuals
      from {{ ref('stg_eurostat__internet_activities_age') }}
      group by age_group, age_label, activity, year
  ),

  online_buying as (
      select
          age_group,
          age_label,
          year,
          pct_buying_online
      from {{ ref('stg_eurostat__online_buying_age') }}
  )

  select
      i.age_group,
      i.age_label,
      i.activity,
      i.year,
      i.pct_individuals,
      o.pct_buying_online
  from internet_activities i
  left join online_buying o
      on i.age_group = o.age_group
      and i.year = o.year
  order by i.age_label, i.year, i.activity
