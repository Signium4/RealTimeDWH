import datetime
import string
import random as r
from faker import Faker

fake = Faker('ru_RU')

def generate_family_name():
    return fake.first_name()

def generate_name():
    return fake.last_name()

def generate_middle_name():
    return fake.middle_name()

def generate_email():
    username = ''.join(r.choice(string.ascii_lowercase) for _ in range(r.randint(5, 10)))
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'mail.ru', 'yandex.ru']
    return f"{username}@{r.choice(domains)}"

def generate_phone():
    return f"+7{r.randint(900, 999)}{r.randint(100, 999)}{r.randint(10, 99)}{r.randint(10, 99)}"

def generate_tax_id():
    return ''.join([str(r.randint(0, 9)) for _ in range(12)])

def generate_date(min=18, max=70):
    end_date = datetime.datetime.now() - datetime.timedelta(days=min * 365)
    start_date = end_date - datetime.timedelta(days=(max - min) * 365)
    random_days = r.randint(0, (end_date - start_date).days)
    return str((start_date + datetime.timedelta(days=random_days)).strftime("%Y-%m-%d"))

def generate_gender():
    return r.choice(['М', 'Ж'])

def generate_status():
    return r.choice(['активный', 'неактивный'])

def generate_country():
    return fake.country()

def generate_city():
    return fake.city()

def generate_account_type():
    account_types = ['дебетовый', 'кредитный', 'депозитный', 'ссудный', 'сберегательный']
    weights = [0.6, 0.2, 0.1, 0.05, 0.05]
    return r.choices(account_types, weights=weights, k=1)[0]

def generate_account_number(bank_code='12345') -> str:
    random_digits = ''.join([str(r.randint(0, 9)) for _ in range(15)])
    account_number = f"{bank_code}{random_digits}"
    return account_number

def generate_transaction_date():
    start_date = datetime.datetime(2025, 1, 1)
    end_date = datetime.datetime(2025, 1, 31, 23, 59, 59)
    delta_seconds = int((end_date - start_date).total_seconds())
    random_seconds = r.randint(0, delta_seconds)
    return start_date + datetime.timedelta(seconds=random_seconds)

def generate_transaction_status():
    statuses = ['pending', 'completed', 'failed', 'reversed']
    weights = [0.6, 0.3, 0.05, 0.05]
    return r.choices(statuses, weights=weights, k=1)[0]

def generate_int_value():
    return r.randint(0, 100)

def generate_str_value():
    return ''.join(
        r.choice(
            string.ascii_uppercase
            + string.digits
            + string.ascii_lowercase
            + string.ascii_letters
        ) for _ in range(r.randint(5, 20))
    )

def generate_double_value():
    return round(r.random() * r.randint(10, 100), 2)