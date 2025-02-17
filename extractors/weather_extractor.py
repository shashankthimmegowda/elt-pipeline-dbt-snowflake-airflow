"""OpenWeather API data extractor."""

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

logger = structlog.get_logger(__name__)

# Major US cities + global cities for diversity
DEFAULT_CITIES = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"name": "Houston", "lat": 29.7604, "lon": -95.3698},
    {"name": "Phoenix", "lat": 33.4484, "lon": -112.0740},
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194},
    {"name": "Seattle", "lat": 47.6062, "lon": -122.3321},
    {"name": "Denver", "lat": 39.7392, "lon": -104.9903},
    {"name": "Miami", "lat": 25.7617, "lon": -80.1918},
    {"name": "Boston", "lat": 42.3601, "lon": -71.0589},
    {"name": "Atlanta", "lat": 33.7490, "lon": -84.3880},
    {"name": "Dallas", "lat": 32.7767, "lon": -96.7970},
    {"name": "Minneapolis", "lat": 44.9778, "lon": -93.2650},
    {"name": "Portland", "lat": 45.5152, "lon": -122.6784},
    {"name": "Austin", "lat": 30.2672, "lon": -97.7431},
    {"name": "Nashville", "lat": 36.1627, "lon": -86.7816},
    {"name": "Detroit", "lat": 42.3314, "lon": -83.0458},
    {"name": "Philadelphia", "lat": 39.9526, "lon": -75.1652},
    {"name": "Washington DC", "lat": 38.9072, "lon": -77.0369},
    {"name": "Las Vegas", "lat": 36.1699, "lon": -115.1398},
    # International
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"name": "Toronto", "lat": 43.6532, "lon": -79.3832},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"name": "São Paulo", "lat": -23.5505, "lon": -46.6333},
    {"name": "Singapore", "lat": 1.3521, "lon": 103.8198},
    {"name": "Dubai", "lat": 25.2048, "lon": 55.2708},
    {"name": "Seoul", "lat": 37.5665, "lon": 126.9780},
    {"name": "Mexico City", "lat": 19.4326, "lon": -99.1332},
    {"name": "Bangkok", "lat": 13.7563, "lon": 100.5018},
    {"name": "Istanbul", "lat": 41.0082, "lon": 28.9784},
    {"name": "Cairo", "lat": 30.0444, "lon": 31.2357},
    {"name": "Lagos", "lat": 6.5244, "lon": 3.3792},
    {"name": "Buenos Aires", "lat": -34.6037, "lon": -58.3816},
    {"name": "Cape Town", "lat": -33.9249, "lon": 18.4241},
    {"name": "Amsterdam", "lat": 52.3676, "lon": 4.9041},
    {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686},
    {"name": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"name": "Beijing", "lat": 39.9042, "lon": 116.4074},
    {"name": "Hong Kong", "lat": 22.3193, "lon": 114.1694},
    {"name": "Taipei", "lat": 25.0330, "lon": 121.5654},
    {"name": "Jakarta", "lat": -6.2088, "lon": 106.8456},
    {"name": "Nairobi", "lat": -1.2921, "lon": 36.8219},
    {"name": "Lima", "lat": -12.0464, "lon": -77.0428},
    {"name": "Auckland", "lat": -36.8485, "lon": 174.7633},
    {"name": "Dublin", "lat": 53.3498, "lon": -6.2603},
    {"name": "Zurich", "lat": 47.3769, "lon": 8.5417},
]

# OpenWeather free tier: 60 calls/min
RATE_LIMIT_DELAY = 1.1


class WeatherExtractor:
    """Extracts current weather and forecast data from OpenWeather API."""

    def __init__(self):
        settings = get_settings()
        self.api_key = settings.weather.api_key
        self.base_url = settings.weather.base_url
        self.extracted_at = datetime.now(timezone.utc).isoformat()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def _fetch_current_weather(
        self, client: httpx.AsyncClient, city: dict
    ) -> dict[str, Any] | None:
        """Fetch current weather for a single city."""
        try:
            resp = await client.get(
                f"{self.base_url}/weather",
                params={
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "appid": self.api_key,
                    "units": "metric",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            return {
                "city_name": city["name"],
                "latitude": city["lat"],
                "longitude": city["lon"],
                "temperature_c": data["main"]["temp"],
                "feels_like_c": data["main"]["feels_like"],
                "temp_min_c": data["main"]["temp_min"],
                "temp_max_c": data["main"]["temp_max"],
                "pressure_hpa": data["main"]["pressure"],
                "humidity_pct": data["main"]["humidity"],
                "visibility_m": data.get("visibility"),
                "wind_speed_ms": data["wind"]["speed"],
                "wind_deg": data["wind"].get("deg"),
                "wind_gust_ms": data["wind"].get("gust"),
                "clouds_pct": data["clouds"]["all"],
                "weather_main": data["weather"][0]["main"],
                "weather_description": data["weather"][0]["description"],
                "weather_icon": data["weather"][0]["icon"],
                "rain_1h_mm": data.get("rain", {}).get("1h", 0),
                "rain_3h_mm": data.get("rain", {}).get("3h", 0),
                "snow_1h_mm": data.get("snow", {}).get("1h", 0),
                "snow_3h_mm": data.get("snow", {}).get("3h", 0),
                "sunrise_utc": datetime.fromtimestamp(
                    data["sys"]["sunrise"], tz=timezone.utc
                ).isoformat(),
                "sunset_utc": datetime.fromtimestamp(
                    data["sys"]["sunset"], tz=timezone.utc
                ).isoformat(),
                "timezone_offset_s": data["timezone"],
                "observation_dt_utc": datetime.fromtimestamp(
                    data["dt"], tz=timezone.utc
                ).isoformat(),
                "extracted_at": self.extracted_at,
            }

        except httpx.HTTPStatusError as e:
            logger.error(
                "weather_fetch_failed",
                city=city["name"],
                status=e.response.status_code,
                error=str(e),
            )
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def _fetch_air_quality(
        self, client: httpx.AsyncClient, city: dict
    ) -> dict[str, Any] | None:
        """Fetch air quality data for a city."""
        try:
            resp = await client.get(
                f"{self.base_url.replace('/2.5', '/2.5')}/air_pollution",
                params={
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "appid": self.api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("list"):
                return None

            aqi_data = data["list"][0]
            components = aqi_data.get("components", {})

            return {
                "city_name": city["name"],
                "latitude": city["lat"],
                "longitude": city["lon"],
                "aqi": aqi_data["main"]["aqi"],
                "co": components.get("co"),
                "no": components.get("no"),
                "no2": components.get("no2"),
                "o3": components.get("o3"),
                "so2": components.get("so2"),
                "pm2_5": components.get("pm2_5"),
                "pm10": components.get("pm10"),
                "nh3": components.get("nh3"),
                "observation_dt_utc": datetime.fromtimestamp(
                    aqi_data["dt"], tz=timezone.utc
                ).isoformat(),
                "extracted_at": self.extracted_at,
            }
        except httpx.HTTPStatusError:
            return None

    async def extract_current_weather(
        self,
        cities: list[dict] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract current weather for all cities with rate limiting."""
        cities = cities or DEFAULT_CITIES
        results = []

        async with httpx.AsyncClient(timeout=30) as client:
            # Process in batches to respect rate limits
            batch_size = 50
            for i in range(0, len(cities), batch_size):
                batch = cities[i : i + batch_size]
                tasks = [self._fetch_current_weather(client, city) for city in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in batch_results:
                    if isinstance(result, dict):
                        results.append(result)
                    elif isinstance(result, Exception):
                        logger.error("batch_fetch_error", error=str(result))

                # Rate limit between batches
                if i + batch_size < len(cities):
                    await asyncio.sleep(RATE_LIMIT_DELAY * batch_size)

        logger.info("weather_extraction_complete", cities_extracted=len(results))
        return results

    async def extract_air_quality(
        self,
        cities: list[dict] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract air quality data for all cities."""
        cities = cities or DEFAULT_CITIES
        results = []

        async with httpx.AsyncClient(timeout=30) as client:
            for city in cities:
                result = await self._fetch_air_quality(client, city)
                if result:
                    results.append(result)
                await asyncio.sleep(RATE_LIMIT_DELAY)

        logger.info("air_quality_extraction_complete", cities_extracted=len(results))
        return results

    async def extract_all(
        self, cities: list[dict] | None = None
    ) -> dict[str, list[dict[str, Any]]]:
        """Extract all weather data types."""
        weather = await self.extract_current_weather(cities)
        air_quality = await self.extract_air_quality(cities)

        return {
            "current_weather": weather,
            "air_quality": air_quality,
        }
