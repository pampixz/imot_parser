import psycopg2
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import os
from dotenv import load_dotenv

load_dotenv()

class PostgresPipeline:
    """
    Scrapy pipeline для сохранения данных в PostgreSQL.
    Использует параметры подключения из переменных окружения:
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    """

    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        # Проверка обязательных env переменных
        required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
        missing_vars = [v for v in required_vars if not os.getenv(v)]
        if missing_vars:
            spider.logger.error(f"❌ Отсутствуют обязательные переменные окружения: {missing_vars}")
            raise RuntimeError(f"Missing environment variables: {missing_vars}")

        try:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 5432)),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                dbname=os.getenv("DB_NAME"),
            )
            self.cur = self.conn.cursor()
            self.create_table()
            spider.logger.info("✅ Подключение к PostgreSQL установлено.")
        except Exception as e:
            spider.logger.error(f"❌ Ошибка подключения к БД: {e}")
            self.conn = None
            self.cur = None
            raise e

    def close_spider(self, spider):
        if self.conn:
            try:
                self.conn.commit()  # один коммит в конце работы паука
                if self.cur:
                    self.cur.close()
                self.conn.close()
                spider.logger.info("🔌 Соединение с БД закрыто.")
            except Exception as e:
                spider.logger.error(f"❌ Ошибка при закрытии соединения: {e}")

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS sofia_apartments (
            id SERIAL PRIMARY KEY,
            title TEXT,
            price NUMERIC,
            currency TEXT,
            price_sqm NUMERIC,
            area NUMERIC,
            floor TEXT,
            construction_type TEXT,
            year_built INTEGER,
            description TEXT,
            location TEXT,
            district TEXT,
            city TEXT,
            agency TEXT,
            phone TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            url TEXT
        )
        """
        self.cur.execute(query)

    def process_item(self, item, spider):
        if not self.conn or not self.cur:
            spider.logger.warning("⚠️ Пропущен item — нет подключения к БД.")
            raise DropItem("Отсутствует соединение с базой данных.")

        adapter = ItemAdapter(item)
        source_id = adapter.get('source_id')
        try:
            data = self.prepare_data(adapter)
            insert_query = """
            INSERT INTO sofia_apartments (
                title, price, currency, price_sqm, area,
                floor, construction_type, year_built, description,
                location, district, city, agency, phone, url
            ) VALUES (
                %(title)s, %(price)s, %(currency)s, %(price_sqm)s, %(area)s,
                %(floor)s, %(construction_type)s, %(year_built)s, %(description)s,
                %(location)s, %(district)s, %(city)s, %(agency)s, %(phone)s, %(url)s
            )
            ON CONFLICT (source_id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                currency = EXCLUDED.currency,
                price_sqm = EXCLUDED.price_sqm,
                area = EXCLUDED.area,
                floor = EXCLUDED.floor,
                construction_type = EXCLUDED.construction_type,
                year_built = EXCLUDED.year_built,
                description = EXCLUDED.description,
                location = EXCLUDED.location,
                district = EXCLUDED.district,
                city = EXCLUDED.city,
                agency = EXCLUDED.agency,
                phone = EXCLUDED.phone,
                url = EXCLUDED.url
            """
            self.cur.execute(insert_query, data)
            # не коммитим здесь, коммит в close_spider
            spider.logger.info(f"✅ Объявление обновлено/сохранено: {source_id}")
        except Exception as e:
            self.conn.rollback()
            spider.logger.error(f"❌ Ошибка при вставке {source_id}: {e}")
            raise DropItem(f"Ошибка вставки в БД: {e}")

        return item

    def prepare_data(self, adapter):
        scraped_at = adapter.get('scraped_at')


        return {
            'title': adapter.get('title'),
            'price': adapter.get('price'),
            'currency': adapter.get('currency'),
            'price_sqm': adapter.get('price_sqm'),
            'area': adapter.get('area'),
            'floor': adapter.get('floor'),
            'construction_type': adapter.get('construction_type'),
            'year_built': adapter.get('year_built'),
            'description': adapter.get('description'),
            'location': adapter.get('location'),
            'district': adapter.get('district'),
            'city': adapter.get('city'),
            'agency': adapter.get('agency'),
            'phone': adapter.get('phone'),
            'url': adapter.get('url'),
        }
