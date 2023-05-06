{{
    config(
        materialized='ephemeral'
    )
}}

with hourly as (
    select * from {{ ref('stg_weather_current') }}
),

daily as (
    select
        city_name,
        latitude,
        longitude,
        date_trunc('day', observed_at) as observation_date,

        -- Temperature
        avg(temperature_c) as avg_temperature_c,
        min(temp_min_c) as min_temperature_c,
        max(temp_max_c) as max_temperature_c,
        max(temp_max_c) - min(temp_min_c) as temperature_range_c,

        -- Conditions
        avg(humidity_pct) as avg_humidity_pct,
        avg(pressure_hpa) as avg_pressure_hpa,
        avg(wind_speed_ms) as avg_wind_speed_ms,
        max(wind_speed_ms) as max_wind_speed_ms,
        avg(clouds_pct) as avg_cloud_cover_pct,

        -- Precipitation
        sum(rain_1h_mm) as total_rain_mm,
        sum(snow_1h_mm) as total_snow_mm,
        sum(rain_1h_mm) + sum(snow_1h_mm) as total_precipitation_mm,

        -- Dominant weather
        mode(weather_condition) as dominant_weather_condition,

        -- Observation count for quality
        count(*) as observation_count,

        -- Daylight
        min(sunrise_at) as sunrise_at,
        max(sunset_at) as sunset_at,
        datediff('minute', min(sunrise_at), max(sunset_at)) as daylight_minutes

    from hourly
    group by 1, 2, 3, 4
)

select * from daily
