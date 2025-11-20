with
    current_flat as (
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

    forecast_flat as (
        select
            record_time_utc,
            'forecast' as record_type,
            cast(null as datetime) as sunrise,
            cast(null as datetime) as sunset,
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

    )

    (
        select *
        from current_flat
        union all
        select *
        from forecast_flat
    )
order by record_time_utc, record_type
