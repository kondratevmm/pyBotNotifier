import auth
from tinkoff.invest import Client, PortfolioResponse

TOKEN = auth.INVEST_TOKEN

def getAccountsAmounts():
    def getAccounts():
        with Client(TOKEN) as client:
            response = client.users.get_accounts()
            accounts = response.accounts # извлекаем список аккаунтов `accounts` из этого ответа
            return accounts

    accounts = getAccounts()
    account_info_short = [(account.id, account.name) for account in accounts if account.name != 'Инвесткопилка'] # Исплючаем Инвесткопилку т.к. при дальнейшей работе с ней сыпятся ошибки

    def getPortfolio(account_id):
        with Client(TOKEN) as client:
            portfolio = client.operations.get_portfolio(
                account_id=account_id)
            return(int(portfolio.total_amount_portfolio.units))

    account_info_long = []
    for account_id, account_name in account_info_short:
        portfolio_value = getPortfolio(account_id)
        account_info_long.append((account_id, account_name, portfolio_value))

    return(account_info_long)