with
    current_records as (
        select
            record_time_utc,
            'current' as record_type,
            sunrise_utc,
            sunset_utc,
            temp,
            feels_like,
            weather_main,
            weather_description,
            pressure_hpa,
            humidity,
            visibility_m,
            clouds_pct,
            wind_speed,
            wind_deg,
            wind_gust,
            rain_1h_mm as rain_mm,
            snow_1h_mm as snow_mm,
            is_raining,
            is_snowing,
            fetched_at
        from {{ ref("stg_current") }}
    ),

    forecast_records as (
        select
            record_time_utc,
            'forecast' as record_type,
            cast(null as datetime) as sunrise_utc,
            cast(null as datetime) as sunset_utc,
            temp,
            feels_like,
            weather_main,
            weather_description,
            pressure_hpa,
            humidity,
            visibility_m,
            clouds_pct,
            wind_speed,
            wind_deg,
            wind_gust,
            rain_3h_mm as rain_mm,
            snow_3h_mm as snow_mm,
            is_raining,
            is_snowing,
            fetched_at
        from {{ ref("stg_forecast") }}
    ),

    unioned as (
        select * from current_records
        union all
        select * from forecast_records
    ),

    deduplicated as (
        select
            *,
            current_timestamp() as loaded_at
        from unioned
        qualify row_number() over (
            partition by record_time_utc, record_type, fetched_at
            order by fetched_at desc  
        ) = 1
    )

select * from deduplicated
order by record_time_utc, record_type