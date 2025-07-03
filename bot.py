import logging
import os
import sys
import asyncio
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler
)
from dotenv import load_dotenv
from excel_exporter import ExcelExporter

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SPIDER_SCRIPT = os.path.join(SCRIPT_DIR, "run_spider_async.py")

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Загрузка .env
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.critical("❌ BOT_TOKEN не найден в .env")
    raise ValueError("BOT_TOKEN не найден в .env")

SELECTING_ACTION, SELECTING_DISTRICT = range(2)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    logger.info(f"/start от {user.id}")
    return await show_action_menu(update, context, user)

async def show_action_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user) -> int:
    keyboard = [
        [InlineKeyboardButton("📦 Из базы", callback_data="from_cache")],
        [InlineKeyboardButton("🔄 Новый поиск", callback_data="new_search")]
    ]
    markup = InlineKeyboardMarkup(keyboard)

    text = f"Привет, {user.first_name}! 👋\nВыберите действие:"
    if update.message:
        await update.message.reply_text(text, reply_markup=markup)
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, reply_markup=markup)

    return SELECTING_ACTION

# Выбор района
async def select_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["mode"] = query.data

    keyboard = [
        [InlineKeyboardButton("Люлин 5", callback_data="lyulin-5")],
        [InlineKeyboardButton("Дружба 1", callback_data="druzhba-1")],
        [InlineKeyboardButton("Лозенец", callback_data="lozenets")]
    ]
    await query.edit_message_text("🏙 Выберите район:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECTING_DISTRICT

# Обработка района
async def handle_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    district = query.data
    mode = context.user_data.get("mode")
    logger.info(f"Район выбран: {district} | Режим: {mode}")

    loading_msg = await query.edit_message_text("⏳ Обработка...")

    try:
        if mode == "new_search":
            await loading_msg.edit_text("🔍 Запускаю парсинг новых объявлений...")
            logger.info(f"🚀 Запуск Scrapy: {sys.executable} {SPIDER_SCRIPT} sofia {district} true")

            process = await asyncio.create_subprocess_exec(
                sys.executable, SPIDER_SCRIPT, "sofia", district, "true",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            async def log_stream(stream, name):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    logger.info(f"[{name}] {line.decode().rstrip()}")

            await asyncio.gather(
                log_stream(process.stdout, "stdout"),
                log_stream(process.stderr, "stderr")
            )

            return_code = await process.wait()
            if return_code != 0:
                logger.error(f"❌ Паук завершился с ошибкой: код {return_code}")
                await loading_msg.edit_text("❌ Ошибка при парсинге. См. логи.")
                return await offer_restart(update, context)

            logger.info("✅ Паук успешно завершён.")
            await loading_msg.edit_text("✅ Парсинг завершён, формирую отчёт...")

        else:
            logger.info("📦 Режим: из базы, паук не запускается.")
            await loading_msg.edit_text("📦 Получаю данные из базы...")

        exporter = ExcelExporter()
        export_path = exporter.export_to_excel("sofia", district)

        if not export_path or not os.path.exists(export_path):
            logger.warning("⚠️ Отчёт не создан.")
            await loading_msg.edit_text("⚠️ Не удалось сформировать отчёт.")
            return await offer_restart(update, context)

        with open(export_path, "rb") as file:
            await query.message.reply_document(
                document=file,
                filename=os.path.basename(export_path),
                caption=f"🏡 Результаты для района {district.replace('-', ' ').title()}"
            )

        return await offer_restart(update, context)

    except Exception as e:
        logger.exception("❌ Ошибка при обработке района:")
        await loading_msg.edit_text(f"❌ Внутренняя ошибка: {str(e)}")
        return ConversationHandler.END

# Предложить новый запрос
async def offer_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    keyboard = [[InlineKeyboardButton("🔄 Новый запрос", callback_data="restart")]]
    await update.callback_query.message.reply_text(
        "✅ Готово! Хотите выполнить новый запрос?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SELECTING_ACTION

# Обработка restart
async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    context.user_data.clear()
    user = update.effective_user
    return await show_action_menu(update, context, user)

# Отмена
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("❌ Отменено.")
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text("❌ Отменено.")
    return ConversationHandler.END

# Обработка ошибок
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("⚠️ Необработанная ошибка:", exc_info=context.error)
    try:
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text("⚠️ Произошла ошибка.")
        elif hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.answer("⚠️ Произошла ошибка", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка в error_handler: {e}")

# Установка команд
async def post_init(application):
    await application.bot.set_my_commands([
        ("start", "Начать работу"),
        ("cancel", "Отменить действие")
    ])

# Основной запуск
def main():
    application = ApplicationBuilder() \
        .token(BOT_TOKEN) \
        .post_init(post_init) \
        .build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            SELECTING_ACTION: [
                CallbackQueryHandler(select_district, pattern="^(from_cache|new_search)$"),
                CallbackQueryHandler(restart, pattern="^restart$")
            ],
            SELECTING_DISTRICT: [CallbackQueryHandler(handle_district)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_message=False
    )

    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    logger.info("✅ Бот запущен")
    application.run_polling()

if __name__ == "__main__":
    main()


