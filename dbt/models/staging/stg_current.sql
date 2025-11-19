select
    datetime(
    timestamp_seconds(cast(json_value(data, '$.dt') as int64))) as record_time_utc,

    datetime(
    timestamp_seconds(cast(json_value(data, '$.sys.sunrise') as int64))) as sunrise_utc,

    datetime(timestamp_seconds(cast(json_value(data, '$.sys.sunset') as int64))) as sunset_utc,

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
    cast(json_value(data, '$.rain.1h') as float64) as rain_1h_mm,
    cast(json_value(data, '$.snow.1h') as float64) as snow_1h_mm,
    (coalesce(cast(json_value(data, '$.rain.1h') as float64), 0) > 0) as is_raining,
    (coalesce(cast(json_value(data, '$.snow.1h') as float64), 0) > 0) as is_snowing,
    fetched_at
from {{ source("raw", "current_raw") }}
