from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher.filters.state import State, StatesGroup
import sqlite3
import logging
from tabulate import tabulate
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date
import ast
import auth
import invest_requests
import aiosqlite
import asyncio

API_TOKEN = auth.BOT_TOKEN
# конфигурация логгера
logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

aitime = datetime

class States(StatesGroup):
    account_id = State()
    new_rate = State()


db_path = "invest.db"

async def on_startup(dp):
    print("Бот запущен")
    # Создать таблицы базы данных при старте приложения.
    async with aiosqlite.connect(db_path) as db:
        c = await db.cursor()
        await c.execute(
            '''CREATE TABLE IF NOT EXISTS Users 
            (id INTEGER PRIMARY KEY, 
            telegram_id INTEGER UNIQUE NOT NULL)'''
        )
        await c.execute(
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
            last_notification_date STRING)'''
        )
        await db.commit()

def add_user_to_db(user_id):
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("INSERT INTO Users (telegram_id) VALUES (?)", (user_id,))
            conn.commit()
        except sqlite3.IntegrityError:
            return False
    return True

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    user_not_exists =  add_user_to_db(message.from_user.id)
    if user_not_exists:
        return await message.reply("Добро пожаловать. Воспользуйся меню или командой /help для продолжения")
    else:
        return await message.reply("Рад снова тебя видеть, чем займёмся сегодня, Брэйн?")


@dp.message_handler(commands=["getAccountsData"])
async def get_data(message: types.Message):
    async with aiosqlite.connect(db_path) as db:
        c = await db.cursor()
        await c.execute(
            "SELECT * FROM Accounts WHERE telegram_id=?", (message.from_user.id,)
        )
        accounts = await c.fetchall()
        # Проверяем, есть ли уже какие-то данные
        if accounts:
            msg = "У вас уже есть данные. Хотите ли вы перезаписать их?"
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Да", callback_data="rewrite"))
            markup.add(types.InlineKeyboardButton("Нет", callback_data="abort"))
            return await bot.send_message(chat_id=message.chat.id, text=msg, reply_markup=markup)
        else:
            await write_data(message.from_user.id)
        await db.commit()

async def write_data(user_id):
    data = invest_requests.getAccountsAmounts()
    async with aiosqlite.connect(db_path) as db:
        for item in data:
            c = await db.cursor()
            await c.execute(
                "INSERT INTO Accounts (telegram_id,account_id, name, daily_change_rate, amount_rub, last_updated) VALUES (?,?,?,?,?,?)",
                (user_id, item[0], item[1], 0, item[2], datetime.now().date().isoformat()),
            )
        await db.commit()
        return await bot.send_message(chat_id=user_id, text="Данные успешно сохранены.")

@dp.message_handler(commands=["getCurrentSettings"])
async def get_current_settings(message: types.Message):
    async with aiosqlite.connect(db_path) as db:
        c = await db.cursor()
        await c.execute(
            "SELECT account_id, name, daily_change_rate, amount_rub, last_updated FROM Accounts WHERE telegram_id=?", (message.from_user.id,)
        )
        accounts = await c.fetchall()
        if accounts:
            headers = ["Account ID", "Name", "Daily Change Rate", "Amount RUB", "Last Updated"]
            table = tabulate(accounts, headers, tablefmt="pipe")
            await db.commit()
            return await bot.send_message(chat_id=message.chat.id, text=f"```\n{table}\n```", parse_mode='Markdown')
        else:
            return await bot.send_message(chat_id=message.chat.id, text="У вас пока нет сохраненных настроек.")

@dp.callback_query_handler(lambda c: c.data in ["rewrite", "abort"])
async def process_callback(callback_query: types.CallbackQuery):
    if callback_query.data == "rewrite":
        async with aiosqlite.connect(db_path) as db:
            c = await db.cursor()
            await c.execute(
                "DELETE FROM Accounts WHERE telegram_id=?", (callback_query.from_user.id,)
            )
            await db.commit()
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
        async with aiosqlite.connect(db_path) as db:
            c = await db.cursor()
            await c.execute('SELECT * FROM Accounts WHERE account_id=?', (account_id,))
            account = await c.fetchone()
            await db.commit()
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
            async with aiosqlite.connect(db_path) as db:
                c = await db.cursor()
                await c.execute('SELECT daily_change_rate FROM Accounts WHERE account_id=?', (account_id,))
                rate = await c.fetchone()
                await db.commit()
                await message.answer(f"Текущий rate для данного портфеля: {rate[0]}%")
        elif account_action == "setRate":
            await States.new_rate.set()
            await message.answer("Укажите процентное изменение портфеля, при котором хотите получать уведомление")
        elif account_action == "discardRate":
            async with aiosqlite.connect(db_path) as db:
                c = await db.cursor()
                await c.execute('UPDATE Accounts SET daily_change_rate=0.0 WHERE account_id=?', (account_id,))
                await db.commit()
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
        async with aiosqlite.connect(db_path) as db:
            c = await db.cursor()
            await c.execute('UPDATE Accounts SET daily_change_rate=? WHERE account_id=?', (new_rate, account_id))
            await db.commit()
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

# async def check_changes():
#
#     # Здесь мы получим все аккаунты, где daily_change_rate не равен 0.0
#     async with aiosqlite.connect(db_path) as db:
#         c = await db.cursor()
#         await c.execute('SELECT * FROM Accounts WHERE daily_change_rate != 0.0')
#         accounts = await c.fetchall()
#         await db.commit()
#
#         accounts_amounts = invest_requests.getAccountsAmounts()
#
#         for account in accounts:
#             account_id = account[2]
#             old_amount_rub = account[5]
#             daily_change_rate = account[4]
#
#             found = False
#             for acc in accounts_amounts:
#                 if ast.literal_eval(acc[0]) == account_id:
#                     new_amount_rub = acc[2]
#                     found = True
#                     break
#
#             if not found:
#                 print(f"Нет данных для аккаунта {account_id}")
#                 continue
#
#             if old_amount_rub is None:
#                 # Пропустить аккаунт, если это первый запуск работы
#                 print(f"Пропускаем аккаунт {account_id}, так как поле со стоимостью не заполнено")
#                 continue
#
#             # Рассчитать actual_change_rate
#             actual_change_rate = (new_amount_rub / old_amount_rub - 1)*100
#             # Распаковываю информацию для удобства
#             telegram_id = account[1]
#             last_notification_date = account[9]
#
#             if last_notification_date is None:
#                 last_notification_date = date(1970, 1, 1)  # установить дату по умолчанию
#
#             else:
#                 if isinstance(last_notification_date, str):
#                     last_notification_date = datetime.strptime(last_notification_date, '%Y-%m-%d').date()
#                 else:  # Если это целое число, обработать как timestamp.
#                     last_notification_date = datetime.fromtimestamp(last_notification_date).date()
#             # Проверяем, нужно ли отправить уведомление
#             if last_notification_date != datetime.today().date():
#                 if (daily_change_rate < 0.0 and actual_change_rate < daily_change_rate) \
#                         or (daily_change_rate > 0.0 and actual_change_rate > daily_change_rate):
#                     await c.execute(
#                         'UPDATE Accounts SET amount_rub_notified=?, last_notified_change=?, last_notification_date=? WHERE account_id=?',
#                         (new_amount_rub, actual_change_rate, datetime.today().date().strftime('%Y-%m-%d'), account_id))
#                     await db.commit()
#                     await bot.send_message(telegram_id,
#                                            f"Изменение за день по портфелю {account[3]} превысило установленные {daily_change_rate}% и составило {round(actual_change_rate, 3)}%")
#                     print(f"Уведомление отправлено для аккаунта {account_id}")
#             else:
#                 print(f"Уведомление для аккаунта {account_id} не будет отправлено т.к. сегодня уже высылалось")

async def get_accounts_from_db():
    async with aiosqlite.connect(db_path) as db:
        c = await db.cursor()
        await c.execute('SELECT * FROM Accounts WHERE daily_change_rate != 0.0')
        accounts = await c.fetchall()
        await db.commit()
    return accounts


async def get_accounts_amounts():
    try:
        accounts_amounts = invest_requests.getAccountsAmounts()
        return accounts_amounts
    except Exception as e:
        print(f"Ошибка при получении данных аккаунтов: {str(e)}")
        return []


async def process_accounts(accounts, accounts_amounts):
    tasks = []
    for account in accounts:
        account_id = account[2]
        old_amount_rub = account[5]
        daily_change_rate = account[4]

        new_amount_rub = find_new_amount_rub(account_id, accounts_amounts)

        if old_amount_rub is None:
            print(f"Пропускаем аккаунт {account_id}, так как поле со стоимостью не заполнено")
            continue

        actual_change_rate = (new_amount_rub / old_amount_rub - 1) * 100
        if check_if_notification_needed(account, actual_change_rate, daily_change_rate):
            tasks.append(update_account_information(account_id, new_amount_rub, actual_change_rate))
            tasks.append(notify_user(account, actual_change_rate))

    await asyncio.gather(*tasks)


def find_new_amount_rub(account_id, accounts_amounts):
    for acc in accounts_amounts:
        if ast.literal_eval(acc[0]) == account_id:
            return acc[2]
    print(f"Нет данных для аккаунта {account_id}")
    return None


def check_if_notification_needed(account, actual_change_rate, daily_change_rate):
    last_notification_date = account[9]
    if last_notification_date is None:
        last_notification_date = date(1970, 1, 1)

    else:
        if isinstance(last_notification_date, str):
            last_notification_date = datetime.strptime(last_notification_date, '%Y-%m-%d').date()
        else:
            last_notification_date = datetime.fromtimestamp(last_notification_date).date()

    if last_notification_date == datetime.today().date():
        print(f"Уведомление по портфелю {account[2]} не будет отправлено, так как сегодня уже отправлялось.")
        return False

    return ((daily_change_rate < 0.0 and actual_change_rate < daily_change_rate)
            or (daily_change_rate > 0.0 and actual_change_rate > daily_change_rate))


async def update_account_information(account_id, new_amount_rub, actual_change_rate):
    async with aiosqlite.connect(db_path) as db:
        c = await db.cursor()
        await c.execute(
            'UPDATE Accounts SET amount_rub_notified=?, last_notified_change=?, last_notification_date=? WHERE account_id=?',
            (new_amount_rub, actual_change_rate, datetime.today().date().strftime('%Y-%m-%d'), account_id))
        await db.commit()


async def notify_user(account, actual_change_rate):
    telegram_id = account[1]
    daily_change_rate = account[4]
    await bot.send_message(telegram_id,
                           f"Изменение за день по портфелю {account[3]} превысило установленные {daily_change_rate}% и составило {round(actual_change_rate, 3)}%")
    print(f"Уведомление отправлено для аккаунта {account[2]}")


async def check_changes():
    accounts = await get_accounts_from_db()
    accounts_amounts = await get_accounts_amounts()
    await process_accounts(accounts, accounts_amounts)

'''
Блок с 2 job'ой
'''
async def update_all_accounts():
    data = invest_requests.getAccountsAmounts()
    async with aiosqlite.connect(db_path) as db:
        c = await db.cursor()
        await c.execute('SELECT * FROM Accounts')
        accounts = await c.fetchall()
        await db.commit()

        for account in accounts:
            account_id = account[2]
            for item in data:
                if item[0] == str(account_id):
                    print(f"Update {account_id}: {item[2]}")
                    await c.execute(
                        "UPDATE Accounts SET amount_rub = ?, last_updated = ?, amount_rub_notified = NULL, last_notified_change = NULL, last_notification_date = NULL WHERE account_id = ?",
                        (item[2], datetime.now().date().isoformat(), account_id)
                    )
                    await db.commit()

        print("Закончили обновление")

scheduler = AsyncIOScheduler()
scheduler.add_job(check_changes, 'interval', seconds=30)
scheduler.add_job(update_all_accounts, 'cron', hour=23, minute=50)
scheduler.start()

if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
