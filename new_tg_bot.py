import asyncio
import logging
import os
import subprocess
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup
import sys
from dotenv import load_dotenv

# Подключаем переменные окружения
load_dotenv()

# Включаем логирование для всех уровней сообщений
logging.basicConfig(level=logging.INFO)

# Логгер для отправки сообщений об ошибках в чат
error_logger = logging.getLogger(__name__)
error_logger.setLevel(logging.ERROR)

# Обработчик логов для отправки сообщений в Telegram
class TelegramHandler(logging.Handler):
    def __init__(self, chat_id, bot):
        super().__init__()
        self.chat_id = chat_id
        self.bot = bot

    def emit(self, record):
        # Отправка логов уровня ERROR и выше в Telegram
        log_entry = self.format(record)
        asyncio.create_task(self.bot.send_message(self.chat_id, f"Ошибка в процессе работы программы:\n\n{log_entry}"))

# Инициализация бота
bot = Bot(token=os.getenv('BOT_TOKEN'))
dp = Dispatcher()

# Переменная для хранения PID процесса
process = None
# ID чата для отправки ошибок
ERROR_CHAT_ID = os.getenv('LOGS_CHAT')

# Создаем и добавляем обработчик для отправки логов уровня ERROR в Telegram
telegram_handler = TelegramHandler(ERROR_CHAT_ID, bot)
telegram_handler.setFormatter(logging.Formatter(
    "[%(asctime)s.%(msecs)03d] %(module)10s:%(lineno)-3d %(levelname)-7s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
error_logger.addHandler(telegram_handler)


# Текстовые команды, которые может обрабатывать бот
start_commands = ['Старт ✅', 'Старт', 'старт', 'начать', 'start']
stop_commands = ['Стоп ❌', 'Стоп', 'СТОП', 'стоп', 'закончить', 'stop']
info_commands = ['Информация о боте ℹ️', 'информация о боте', 'информация', 'инфо', 'info']
text_commands = start_commands + stop_commands + info_commands


# Создание клавиатуры
def start_keyboard():
    button_run = KeyboardButton(text='Старт ✅')
    button_stop = KeyboardButton(text='Стоп ❌')
    button_info = KeyboardButton(text='Информация о боте ℹ️')
    button_rows = [[button_run, button_stop], [button_info]]
    hello_markup = ReplyKeyboardMarkup(keyboard=button_rows)
    return hello_markup


# Хэндлер на команду /start
@dp.message(Command("start"))
async def get_chat_id(message: types.Message):
    global process
    chat_id = message.chat.id
    await message.answer(
        text=f'Программа работает над поиском новых перевозок🚛',
        reply_markup=start_keyboard(),
        chat=chat_id
    )

    # Запускаем программу как отдельный процесс с перенаправлением stderr в пайп
    if process is None:
        process = subprocess.Popen(
            [sys.executable, "parser.py"],
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        logging.info(f'Started ati_parser.py with PID: {process.pid}')

        # Асинхронно читаем stderr и отправляем в чат
        await asyncio.create_task(handle_errors(process))


# Хэндлер на команду /stop
@dp.message(Command("stop"))
async def stop_process(message: types.Message):
    global process
    if process is not None:
        process.terminate()  # Завершаем процесс
        process = None
        await message.answer("Работа программы была остановлена")
        logging.info("Stopped ati_parser.py")
    else:
        await message.answer("Программа не запущена")


# Хэндлер на команду /info
@dp.message(Command("info"))
async def info(message: types.Message):
    await message.answer(
'''
Это бот для поиска новых перевозок. 
Используйте команды для управления программой.
''')


# Хэндлер на текстовые команды
@dp.message(lambda message: message.text in text_commands)
async def handle_text_commands(message: types.Message):
    if message.text in start_commands:
        await get_chat_id(message)
    elif message.text in stop_commands:
        await stop_process(message)
    elif message.text in info_commands:
        await info(message)


# Функция для асинхронного чтения ошибок из stderr
async def handle_errors(cur_process):
    while True:
        error_line = await asyncio.to_thread(cur_process.stderr.readline)
        if error_line:
            # Логируем все уровни, но отправляем только ERROR и выше
            logging.info(f'Log from ati_parser.py: {error_line.strip()}')
            if "ERROR" in error_line or "CRITICAL" in error_line:
                error_logger.error(f'{error_line.strip()}')
        else:
            break


# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
