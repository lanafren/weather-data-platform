with
    dist as (
        select distinct weather_main, weather_description
        from {{ ref("int_weather_unified") }}
    )

select
    farm_fingerprint(concat(weather_main, '|', weather_description)) as weather_condition_key,
    weather_main,
    weather_description

from dist
order by weather_main
