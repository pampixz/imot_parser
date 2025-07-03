import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import datetime
import re
import logging
from typing import List, Dict, Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

class ExcelExporter:
    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_name = os.getenv("DB_NAME")

    @staticmethod
    def sanitize_filename(text: str) -> str:
        """Очистка строки для использования в имени файла"""
        text = text.strip().lower()
        return re.sub(r"[^\w\-_.]", "_", text)

    def validate_db_connection(self) -> None:
        """Проверка параметров подключения к БД"""
        if not all([self.db_host, self.db_port, self.db_user, self.db_password, self.db_name]):
            raise EnvironmentError("Не все параметры подключения к БД заданы в .env файле")

    def build_db_uri(self) -> str:
        """Создание строки подключения к БД"""
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def apply_filters(self, query: str, params: dict, filters: dict) -> tuple:
        """Добавление фильтров к SQL запросу"""
        if filters.get("apartment_type"):
            query += " AND LOWER(title) LIKE :apartment_type"
            params["apartment_type"] = f"%{filters['apartment_type'].lower()}%"

        # Дополнительные фильтры, если нужно:
        if filters.get("min_area"):
            try:
                params["min_area"] = float(filters["min_area"])
                query += " AND area >= :min_area"
            except ValueError:
                raise ValueError("Параметр 'min_area' должен быть числом")

        if filters.get("rooms"):
            rooms = filters["rooms"]
            if rooms == "3+":
                query += " AND rooms >= 3"
            else:
                try:
                    rooms_int = int(rooms)
                    query += " AND rooms = :rooms"
                    params["rooms"] = rooms_int
                except ValueError:
                    raise ValueError("Параметр 'rooms' должен быть числом или '3+'")

        if filters.get("balcony") == "yes":
            query += " AND description ILIKE '%балкон%'"

        if filters.get("near_metro") == "yes":
            query += " AND description ILIKE '%метро%'"

        if filters.get("location_side"):
            side = filters["location_side"].lower()
            if side == "south":
                query += " AND (description ILIKE '%юг%' OR description ILIKE '%южн%')"
            elif side == "north":
                query += " AND (description ILIKE '%север%' OR description ILIKE '%северн%')"

        return query, params

    def get_data_from_db(self, city: str, district: str, filters: dict = None) -> pd.DataFrame:
        """Получение данных из базы данных"""
        self.validate_db_connection()
        engine = create_engine(self.build_db_uri())

        base_query = """
        SELECT 
            title, price, currency, price_sqm, area, 
            floor, construction_type, year_built, description,
            district, city, url, agency, phone,
            scraped_at::date AS scraped_date
        FROM sofia_apartments
        WHERE LOWER(city) = :city AND LOWER(district) = :district
        """
        params = {"city": city.lower(), "district": district.lower()}

        if filters:
            base_query, params = self.apply_filters(base_query, params, filters)

        base_query += " ORDER BY scraped_at DESC"

        try:
            with engine.connect() as conn:
                logger.info("🔌 Установлено соединение с БД")
                df = pd.read_sql(text(base_query), conn, params=params)
                logger.info(f"📊 Получено записей: {len(df)}")
                return df
        except Exception as e:
            logger.error(f"❌ Ошибка при запросе к БД: {str(e)}")
            raise RuntimeError(f"Ошибка при запросе к БД: {str(e)}")
        finally:
            engine.dispose()

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Подготовка DataFrame к экспорту"""
        if "scraped_date" in df.columns:
            df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.strftime("%Y-%m-%d")

        if "title" in df.columns:
            df["title"] = df["title"].str.replace(r"^Продава\s*", "", regex=True)
            df = df.rename(columns={
                "title": "Тип недвижемости",
                "price": "Цена",
                "currency": "Валюта",
                "price_sqm": "Цена за м²",
                "area": "Площадь",
                "floor": "Этаж",
                "construction_type": "Тип строителства",
                "year_built": "Год постройки",
                "description": "Описание",
                "district": "Район",
                "city": "Город",
                "url": "Ссылка",
                "agency": "Агентство",
                "phone": "Телефон",
                "scraped_date": "Дата сбора"
            })
        return df

    def export_to_excel(
        self,
        city: str,
        district: str,
        listings: Optional[List[Dict]] = None,
        filters: Optional[dict] = None,
        keyword: Optional[str] = None  # 👈 добавлено
    ) -> str:
        """Экспорт данных в Excel файл"""
        logger.info(f"📤 Начало экспорта: город={city}, район={district}")

        if not city or not district:
            raise ValueError("Параметры 'city' и 'district' обязательны")

        try:
            if listings is not None:
                df = pd.DataFrame(listings)
                if df.empty:
                    raise ValueError("Передан пустой список объявлений")
            else:
                filters = filters or {}
                if keyword and keyword.lower() != "all":
                    filters["apartment_type"] = keyword
                df = self.get_data_from_db(city, district, filters)
                if df.empty:
                    raise ValueError("Нет данных для экспорта")

            df = self.prepare_dataframe(df)

            os.makedirs("exports", exist_ok=True)
            safe_city = self.sanitize_filename(city)
            safe_district = self.sanitize_filename(district)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_city}_{safe_district}_{timestamp}.xlsx"
            filepath = os.path.join("exports", filename)

            df.to_excel(filepath, index=False, engine='openpyxl')
            logger.info(f"✅ Файл успешно сохранен: {filepath}")
            print(f"Экспорт завершён: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"❌ Ошибка при экспорте: {str(e)}")
            raise

def export_to_excel(city: str, district: str, listings: Optional[List[Dict]] = None, filters: Optional[dict] = None, keyword: Optional[str] = None) -> str:
    """Функция-обертка"""
    exporter = ExcelExporter()
    return exporter.export_to_excel(city, district, listings, filters, keyword)

if __name__ == "__main__":
    try:
        result = export_to_excel("sofia", "lyulin-5", keyword="3-СТАЕН")
        print(f"Файл создан: {result}")
    except Exception as e:
        print(f"Ошибка: {str(e)}")
