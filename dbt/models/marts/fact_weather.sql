select
    record_time_utc,
    record_type,
    dim_dt.datetime_key,
    dim_wc.weather_condition_key,
    sunrise_utc,
    sunset_utc,
    temp,
    feels_like,
    pressure_hpa,
    humidity,
    clouds_pct,
    wind_speed,
    wind_deg,
    wind_gust,
    rain_mm,
    snow_mm,
    is_raining,
    is_snowing,
    fetched_at,
    current_timestamp() as loaded_at

from {{ ref("int_weather_unified") }} as int_wu

left join
    {{ ref("dim_datetime") }} as dim_dt
    on cast(format_timestamp('%Y%m%d%H%M%S', int_wu.record_time_utc) as int)
    = dim_dt.datetime_key

left join
    {{ ref("dim_weather_condition") }} as dim_wc
    on int_wu.weather_main = dim_wc.weather_main
    and int_wu.weather_description = dim_wc.weather_description
