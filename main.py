from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher.filters.state import State, StatesGroup
import sqlite3
import logging
from tabulate import tabulate
import auth
import invest_requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, time, date, timedelta
import ast
import asyncio

API_TOKEN = auth.BOT_TOKEN

# конфигурация логгера
logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

class States(StatesGroup):
    account_id = State()
    new_rate = State()

conn = sqlite3.connect("invest.db")

c = conn.cursor()
# создаем таблицу в нашей базе данных SQLite
c.execute(
    '''
    CREATE TABLE IF NOT EXISTS Users 
    (id INTEGER PRIMARY KEY, 
    telegram_id INTEGER NOT NULL UNIQUE)
    ''')
c.execute(
    '''CREATE TABLE IF NOT EXISTS Accounts 
        (id INTEGER PRIMARY KEY, 
        telegram_id INTEGER NOT NULL, 
        account_id STRING NOT NULL, 
        name STRING, 
        daily_change_rate REAL, 
        amount_rub INTEGER,
        last_updated STRING,
        amount_rub_notified INTEGER,
        last_notified_change REAL, 
        last_notification_date STRING)
    ''')

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        c.execute("INSERT INTO Users (telegram_id) VALUES (?)", (message.from_user.id,))
    except sqlite3.IntegrityError:
        return await message.reply("Рад снова тебя видеть, чем займёмся сегодня, Брэйн?")
    conn.commit()
    return await message.reply("Добро пожаловать. Воспользуйся меню или командой /help для продолжения")

@dp.message_handler(commands=["getAccountsData"])
async def get_data(message: types.Message):
    c.execute(
        "SELECT * FROM Accounts WHERE telegram_id=?", (message.from_user.id,)
    )
    accounts = c.fetchall()
    # Проверяем, есть ли уже какие-то данные
    if accounts:
        msg = "У вас уже есть данные. Хотите ли вы перезаписать их?"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("Да", callback_data="rewrite"))
        markup.add(types.InlineKeyboardButton("Нет", callback_data="abort"))
        return await bot.send_message(chat_id=message.chat.id, text=msg, reply_markup=markup)
    else:
        await write_data(message.from_user.id)

async def write_data(user_id):
    data = invest_requests.getAccountsAmounts()
    for item in data:
        c.execute(
            "INSERT INTO Accounts (telegram_id,account_id, name, daily_change_rate, amount_rub, last_updated) VALUES (?,?,?,?,?,?)",
            (user_id, item[0], item[1], 0, item[2], datetime.now().date().isoformat()),
        )
    conn.commit()
    return await bot.send_message(chat_id=user_id, text="Данные успешно сохранены.")

async def update_amounts(account_id):
    data = invest_requests.getAccountsAmounts()
    for item in data:
        if str(item[0]) == account_id:
            print(f"Update {account_id}: {item[2]}")
            c.execute(
                "UPDATE Accounts SET amount_rub = ?, last_updated = ? WHERE account_id = ?",
                (item[2], datetime.now().date().isoformat(), account_id)
            )
            conn.commit()
            break

@dp.message_handler(commands=["getCurrentSettings"])
async def get_current_settings(message: types.Message):
    c.execute(
        "SELECT account_id, name, daily_change_rate, amount_rub, last_updated FROM Accounts WHERE telegram_id=?", (message.from_user.id,)
    )
    accounts = c.fetchall()
    if accounts:
        headers = ["Account ID", "Name", "Daily Change Rate", "Amount RUB", "Last Updated"]
        table = tabulate(accounts, headers, tablefmt="pipe")
        return await bot.send_message(chat_id=message.chat.id, text=f"```\n{table}\n```", parse_mode='Markdown')
    else:
        return await bot.send_message(chat_id=message.chat.id, text="У вас пока нет сохраненных настроек.")

@dp.callback_query_handler(lambda c: c.data in ["rewrite", "abort"])
async def process_callback(callback_query: types.CallbackQuery):
    if callback_query.data == "rewrite":
        c.execute(
            "DELETE FROM Accounts WHERE telegram_id=?", (callback_query.from_user.id,)
        )
        conn.commit()
        await bot.answer_callback_query(callback_query.id)
        return await write_data(callback_query.from_user.id)
    if callback_query.data == "abort":
        await bot.answer_callback_query(callback_query.id)
        return await bot.send_message(callback_query.from_user.id, "Перезапись отменена.")

'''
Тут начинается блок работы с портфолио. Команда choosePortfolio позволяет выполнить после неё 3 прочие команды
'''

@dp.message_handler(Command("choosePortfolio"), state='*')
async def choose_portfolio(message: types.Message, state: FSMContext):
    await message.answer("Укажите id портфеля, к которому хотите применить изменения")
    await States.account_id.set()

@dp.message_handler(state=States.account_id)
async def process_account_state(message: types.Message, state: FSMContext):
    user_data = await state.get_data()

    # Если в данных состояния нет account_id, значит мы ждём от пользователя ввода id
    if 'account_id' not in user_data:
        account_id = message.text
        c.execute('SELECT * FROM Accounts WHERE account_id=?', (account_id,))
        account = c.fetchone()

        if account is None:
            await message.answer("Такого портфеля не найдено, попробуйте проверить список написав /getCurrentSettings")
        else:
            await state.update_data(account_id=account_id)
            await message.answer("Выберите одно из действий",
                                 reply_markup=types.ReplyKeyboardMarkup(resize_keyboard=True).add("getCurrentRate", "setRate", "discardRate"))
    else:  # Если у нас уже есть id, значит мы ждём от пользователя действие над портфелем
        account_id = user_data['account_id']
        account_action = message.text

        if account_action == "getCurrentRate":
            c.execute('SELECT daily_change_rate FROM Accounts WHERE account_id=?', (account_id,))
            rate = c.fetchone()
            await message.answer(f"Текущий rate для данного портфеля: {rate[0]}%")
        elif account_action == "setRate":
            await States.new_rate.set()
            await message.answer("Укажите процентное изменение портфеля, при котором хотите получать уведомление")
        elif account_action == "discardRate":
            c.execute('UPDATE Accounts SET daily_change_rate=0.0 WHERE account_id=?', (account_id,))
            conn.commit()
            await message.answer("Вы сбросили процентное изменение портфеля для данного account_id")
            await state.finish()
        else:
            await message.answer("Неверное действие.")

@dp.message_handler(state=States.new_rate, content_types=types.ContentType.TEXT)
async def confirm_rate(message: types.Message, state: FSMContext):
    try:
        new_rate = float(message.text.replace(',', '.'))
        user_data = await state.get_data()
        account_id = user_data['account_id']
        c.execute('UPDATE Accounts SET daily_change_rate=? WHERE account_id=?', (new_rate, account_id))
        conn.commit()
        await message.answer(f"Вы установили {new_rate}% для данного портфеля в качестве уровня, при котором хотите получать уведомление")
        await state.finish()

    except ValueError:
        await message.answer("Вы указали НЕ число, попробуйте снова")

@dp.message_handler(commands=['help'])
async def send_function_list(message: types.Message):
    function_list = """
    ⭐️ Список всех функций:
    /start - Начало работы, регистрация пользователя
    /getAccountsData - Запросить информацию по портфелям из API. Используйте после start или для сброса данных
    /getCurrentSettings - Получение ваших текущих настроек
    /choosePortfolio - Совершение действий с портфелями. Используйте после первичной настройки
    """
    await bot.send_message(message.chat.id, function_list)

'''
Блок с job'ой
'''

async def job(dp: Dispatcher):

    # Здесь мы получим все аккаунты, чтобы в конце дня обновить их - НА ДАННЫЙ МОМЕНТ НЕ РАБОТАЕТ!!!
    c.execute('SELECT * FROM Accounts')
    accounts = c.fetchall()
    for account in accounts:
        account_id = account[2]

        # await update_amounts(account_id) - нужна для дебага

        current_time = datetime.now()

        if datetime(current_time.year, current_time.month, current_time.day, 23, 50) <= current_time <= datetime(
                current_time.year, current_time.month, current_time.day, 23, 59, 59):
            await update_amounts(account_id)
            print("Обновили значения amount_rub и last_updated в таблицe Accounts")

            # Высчитываем, сколько времени осталось до конца дня
            tomorrow = current_time + timedelta(days=1)
            midnight = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
            sleep_time = (midnight - current_time).total_seconds()
            print("Засыпаем до 00:00:01")
            await asyncio.sleep(sleep_time)  # Останавливаем выполнение программы до 00:00:01 следующего дня

    # Здесь мы получим все аккаунты, где daily_change_rate не равен 0.0
    c.execute('SELECT * FROM Accounts WHERE daily_change_rate != 0.0')
    accounts = c.fetchall()

    accounts_amounts = invest_requests.getAccountsAmounts()

    for account in accounts:
        account_id = account[2]
        old_amount_rub = account[5]
        daily_change_rate = account[4]

        found = False
        for acc in accounts_amounts:
            if ast.literal_eval(acc[0]) == account_id:
                new_amount_rub = acc[2]
                found = True
                break

        if not found:
            print(f"Нет данных для аккаунта {account_id}")
            continue

        if old_amount_rub is None:
            # Пропустить аккаунт, если это первый запуск работы
            print(f"Пропускаем аккаунт {account_id}, так как поле со стоимостью не заполнено")
            continue

        # Рассчитать actual_change_rate
        actual_change_rate = (new_amount_rub / old_amount_rub - 1)*100
        # Распаковываю информацию для удобства
        telegram_id = account[1]
        last_notification_date = account[9]

        if last_notification_date is None:
            last_notification_date = date(1970, 1, 1)  # установить дату по умолчанию

        else:
            if isinstance(last_notification_date, str):
                last_notification_date = datetime.strptime(last_notification_date, '%Y-%m-%d').date()
            else:  # Если это целое число, обработать как timestamp.
                last_notification_date = datetime.fromtimestamp(last_notification_date).date()
        # Проверяем, нужно ли отправить уведомление
        if last_notification_date != datetime.today().date():
            if (daily_change_rate < 0.0 and actual_change_rate < daily_change_rate) \
                    or (daily_change_rate > 0.0 and actual_change_rate > daily_change_rate):
                c.execute(
                    'UPDATE Accounts SET amount_rub_notified=?, last_notified_change=?, last_notification_date=? WHERE account_id=?',
                    (new_amount_rub, actual_change_rate, datetime.today().date().strftime('%Y-%m-%d'), account_id))
                conn.commit()
                await bot.send_message(telegram_id,
                                       f"Изменение за день по портфелю {account[3]} превысило установленные {daily_change_rate}% и составило {round(actual_change_rate, 3)}%")
                print(f"Уведомление отправлено для аккаунта {account_id}")
        else:
            print("Уведомление не будет отправлено т.к. условие не соблюдено")


scheduler = AsyncIOScheduler()
scheduler.add_job(job, 'interval', args=[dp], seconds=30)
scheduler.start()

if __name__ == "__main__":
    from aiogram import executor

    executor.start_polling(dp, skip_updates=True, on_startup=print('Бот запущен'))