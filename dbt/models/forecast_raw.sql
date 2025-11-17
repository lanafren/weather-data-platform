select
datetime(
    timestamp_seconds(cast(json_value(data, '$.dt') as int64)),
    "Europe/Berlin") as record_time,

    cast(json_value(data, '$.main.temp') as float64) as temp,
    cast(json_value(data, '$.main.feels_like') as float64) as feels_like,
    json_value(data, '$.weather[0].main') as weather_main,
    json_value(data, '$.weather[0].description') as weather_description,
    cast(json_value(data, '$.main.pressure') as float64) as pressure_hpa,
    cast(json_value(data, '$.main.humidity') as float64) as humidity,
    cast(json_value(data, '$.visibility') as float64) as visibility_m,
    cast(json_value(data, '$.clouds.all') as float64) as clouds_pct,
    cast(json_value(data, '$.wind.speed') as float64) as wind_speed,
    cast(json_value(data, '$.wind.deg') as float64) as wind_deg,
    cast(json_value(data, '$.wind.gust') as float64) as wind_gust,
    cast(json_value(data, '$.rain.3h') as float64) as rain_3h_mm,
    cast(json_value(data, '$.snow.3h') as float64) as snow_3h_mm,
    json_value(data, '$.rain.3h') is not null as is_raining,
    json_value(data, '$.snow.3h') is not null as is_snowing,
    fetched_at
    

from {{ source('raw', 'forecast_raw')}}