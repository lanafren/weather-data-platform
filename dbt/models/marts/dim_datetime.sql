select distinct
    cast(format_timestamp('%Y%m%d%H%M%S', record_time_utc) as int) as datetime_key,
    date(record_time_utc) as date,
    extract(year from record_time_utc) as year,
    extract(month from record_time_utc) as month,
    extract(day from record_time_utc) as day,
    format_timestamp('%A', record_time_utc) as day_of_week,
    extract(week from record_time_utc) as week_of_year,
    extract(dayofyear from record_time_utc) as day_of_year,
    case
        when
            extract(dayofweek from record_time_utc) = 1
            or extract(dayofweek from record_time_utc) = 7
        then true
        else false
    end as is_weekend,
    extract(hour from record_time_utc) as hour

from {{ ref("int_weather_unified") }}
order by date
