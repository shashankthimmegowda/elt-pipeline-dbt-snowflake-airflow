{{
    config(
        materialized='table',
        unique_key=['city_name', 'observation_date']
    )
}}

with daily_weather as (
    select * from {{ ref('int_weather_daily_summary') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['city_name', 'observation_date']) }} as weather_daily_id,

        city_name,
        latitude,
        longitude,
        observation_date,

        -- Temperature
        round(avg_temperature_c, 1) as avg_temperature_c,
        round(min_temperature_c, 1) as min_temperature_c,
        round(max_temperature_c, 1) as max_temperature_c,
        round(temperature_range_c, 1) as temperature_range_c,

        -- Temperature classification
        case
            when avg_temperature_c < 0 then 'freezing'
            when avg_temperature_c < 10 then 'cold'
            when avg_temperature_c < 20 then 'mild'
            when avg_temperature_c < 30 then 'warm'
            else 'hot'
        end as temperature_category,

        -- Atmospheric
        round(avg_humidity_pct, 0) as avg_humidity_pct,
        round(avg_pressure_hpa, 0) as avg_pressure_hpa,
        round(avg_wind_speed_ms, 1) as avg_wind_speed_ms,
        round(max_wind_speed_ms, 1) as max_wind_speed_ms,
        round(avg_cloud_cover_pct, 0) as avg_cloud_cover_pct,

        -- Precipitation
        round(total_rain_mm, 1) as total_rain_mm,
        round(total_snow_mm, 1) as total_snow_mm,
        round(total_precipitation_mm, 1) as total_precipitation_mm,
        total_precipitation_mm > 0 as had_precipitation,

        -- Weather summary
        dominant_weather_condition,

        -- Daylight
        daylight_minutes,
        round(daylight_minutes / 60.0, 1) as daylight_hours,

        -- Data quality
        observation_count,
        observation_count >= 20 as is_complete_day

    from daily_weather
)

select * from final
