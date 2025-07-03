import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

from imot_bg.spiders.imot_debug import ImotBgSpider
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=1)  # Можно увеличить при необходимости


def run_spider_sync(city: str, district: str):
    """Синхронный запуск паука Scrapy"""
    logger.info(f"🚀 Запускаю парсинг для города: {city}, район: {district}")
    try:
        settings = get_project_settings()
        process = CrawlerProcess(settings)
        process.crawl(ImotBgSpider, city=city, district=district)
        process.start()
        logger.info("✅ Парсинг завершён")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске паука: {e}")
        raise


async def run_spider_async(city: str, district: str, is_new_search=False):
    """
    Асинхронный запуск паука.
    Если is_new_search == True → парсим заново.
    Иначе → берём данные из базы.
    """
    # Приводим is_new_search к bool
    if isinstance(is_new_search, str):
        is_new_search = is_new_search.lower() == "true"

    if is_new_search:
        logger.info(f"🔄 Новый парсинг включен: {city} - {district}")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, run_spider_sync, city, district)
    else:
        logger.info(f"📦 Данные берутся из базы: {city} - {district}")
        # Здесь должна быть логика получения из базы (если нужно)
        pass


if __name__ == "__main__":
    print(">>> run_spider_async started", flush=True)

    import sys
    if len(sys.argv) != 4:
        print("Использование: python run_spider_async.py <city> <district> <is_new_search>")
        sys.exit(1)

    city = sys.argv[1]
    district = sys.argv[2]
    is_new_search = sys.argv[3].lower() == "true"

    asyncio.run(run_spider_async(city, district, is_new_search))

# Пример ручного запуска:
# asyncio.run(run_spider_async("sofia", "lyulin-5", force=True))

