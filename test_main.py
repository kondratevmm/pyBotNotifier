import unittest
from unittest.mock import patch
import sqlite3

import invest_requests
from main import add_user_to_db

db_path_test = ":memory:"

class TestAddUserToDatabase(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.c = self.db.cursor()
        self.c.execute(
        '''CREATE TABLE IF NOT EXISTS Users 
        (id INTEGER PRIMARY KEY, 
        telegram_id INTEGER UNIQUE NOT NULL)'''
        )
        self.db.commit()

    @patch('main.sqlite3.connect')
    def test_add_user(self, mock_sql_connect):
        mock_sql_connect.return_value = self.db
        test_user_id = 999999999

        result = add_user_to_db(test_user_id)

        self.assertTrue(result)

        self.c.execute('SELECT telegram_id FROM Users WHERE telegram_id=?', (test_user_id,))
        user_in_db = self.c.fetchall()

        print("DB Contents: ", user_in_db)

        self.assertIsNotNone(user_in_db)
        self.assertEqual(user_in_db[0][0], test_user_id)

class TestInvestRequests(unittest.TestCase):
    def setUp(self):
        self.accounts = invest_requests.getAccountsAmounts()

    def test_tuple_len(self):
        for account in self.accounts:
            # Проверяем, является ли каждый элемент в списке кортежем из трех элементов
            self.assertEqual(len(account), 3)

    def test_tuple_types(self):
        for account in self.accounts:
            # Проверяем, что первый и второй элементы с типом `str`, и третий элемент типа `int` или `float`
            self.assertIsInstance(account[0], str)
            self.assertIsInstance(account[1], str)
            self.assertTrue(isinstance(account[2], int) or isinstance(account[2], float))

if __name__ == '__main__':
    unittest.main()
