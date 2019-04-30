import csv
import re

from pymongo import MongoClient
import datetime


def db_connect():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.hmwrk_db
    return db


def read_data(csv_file, db):
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        concerts = db.concerts
        for line in reader:
            line['Цена'] = int(line['Цена'])
            line['Дата'] = datetime.datetime(2019, int(line['Дата'][3:]), int(line['Дата'][:-3]), 0, 0)
            concerts.insert_one(line).inserted_id
        for item in concerts.find():
            print(item)


def find_cheapest(db):
    concerts = db.concerts
    for item in concerts.find().sort('Цена').limit(14):
        print(item)


def find_by_name(name, db):
    regex = re.compile('.*' + name + '.*')
    concerts = db.concerts
    for item in concerts.find({'Исполнитель': regex}).sort('Цена'):
        print(item)


def sort_by_date(db):
    concerts = db.concerts
    for item in concerts.find().sort('Дата'):
        print(item)


def main():
    command = input('Введите команду: ' + '\n' +
        '1 - Загрузить данные в бд из CSV-файла' + '\n' +
        '2 - Отсортировать билеты из базы по возрастания цены' + '\n' +
        '3 - Найти билеты по имени исполнителя (в том числе – по подстроке) '
        'и вернуть их по возрастанию цены' + '\n' +
        '4 - Cортировка по дате' + '\n' + ': ')

    database = db_connect()

    if command == '1':
        read_data('artists.csv', database)

    elif command == '2':
        find_cheapest(database)

    elif command == '3':
        name_artist = input('Введите имя Исполнителя: ')
        name_artist = ("'" + name_artist + "'")
        find_by_name(name_artist, database)

    elif command == '4':
        sort_by_date(database)


if __name__ == '__main__':
    main()
