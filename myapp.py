import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
from pathlib import Path
import csv
import requests
from bs4 import BeautifulSoup

# Шаг 1: Загрузка данных
# ( файл 2017_CO2_IntensEL_EEA.csv уже был загружен)
df = pd.read_csv("C:/Users/Zhanna.Konograi/Desktop/Итоговая аттестационная работа/2017_CO2_IntensEL_EEA.csv")

# Шаг 2: Анализ данных и извлечение инсайтов

# Инсайт 1: Среднее значение выбросов CO2 по странам за весь период
insight_1 = df.groupby('CountryLong')['ValueNumeric'].mean().reset_index()
insight_1.columns = ['CountryLong', 'mean_co2_emissions']

# Инсайт 2.1: Максимальные выбросы CO2 по годам
insight_2_1 = df.groupby('Year')['ValueNumeric'].max().reset_index(name='Max_CO2_Emissions')

# Инсайт 2: Максимальные выбросы CO2 по годам в разрезе стран
# Группируем данные по стране и году, считаем сумму выбросов CO2
grouped_df = df.groupby(['CountryLong', 'Year'])['ValueNumeric'].sum().reset_index()

# Сортируем данные по убыванию суммы выбросов CO2 внутри каждого года
sorted_grouped_df = grouped_df.sort_values(by=['Year', 'ValueNumeric'], ascending=[True, False])

# Выводим все страны с наибольшими выбросами для каждого года
insight_2 = sorted_grouped_df

# Инсайт 3: Минимальное значение интенсивности CO2 для каждой страны
insight_3 = df.groupby('CountryLong')['ValueNumeric'].min().reset_index()
insight_3.columns = ['CountryLong', 'Min_CO2_Intensity']

# Инсайт 4: Изменение выбросов CO2 от года к году
df.sort_values(['CountryLong', 'Year'], inplace=True)
df['change_in_co2'] = df.groupby('CountryLong')['ValueNumeric'].diff()
insight_4 = df.query("change_in_co2.notnull()")

# Инсайт 5: Изменение выбросов CO2 со временем для конкретной страны
country = 'United Kingdom'
insight_5 = df.query("CountryLong == @country").groupby('Year')['ValueNumeric'].mean().reset_index()
insight_5.columns = ['Year', 'Mean_ValueNumeric_for_United_Kingdom']

# Шаг 3: Визуализация инсайтов

# Визуализация 1: Средние выбросы CO2 по странам
plt.figure(figsize=(15, 7))
plt.bar(insight_1['CountryLong'], insight_1['mean_co2_emissions'])
plt.xlabel('Страна')
plt.ylabel('Средние выбросы CO2')
plt.title('Средние выбросы CO2 по странам')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# Визуализация 2: Максимальные выбросы CO2 по годам в разрезе стран
plt.figure(figsize=(15, 8))

# Отсортируем данные по годам и добавим линии для каждой страны
for country in sorted_grouped_df['CountryLong'].unique():
    country_data = sorted_grouped_df.query("CountryLong == @country")
    plt.plot(country_data['Year'], country_data['ValueNumeric'], label=country)

# Добавляем заголовок, подписи осей и легенду
plt.title('Максимальные выбросы CO2 по годам в разрезе стран')
plt.xlabel('Год')
plt.ylabel('Выбросы CO2 (сумма)')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

# Визуализация 2.1: Максимальные выбросы CO2 по годам
plt.figure(figsize=(12, 6))
plt.plot(insight_2_1['Year'], insight_2_1['Max_CO2_Emissions'])
plt.xlabel('Год')
plt.ylabel('Максимальные выбросы CO2')
plt.title('Максимальные выбросы CO2 по годам')
plt.grid(True)
plt.tight_layout()
plt.show()

# Визуализация 3: Минимальное значение интенсивности CO2 по регионам
plt.figure(figsize=(20,6))
sns.barplot(x='CountryLong', y='Min_CO2_Intensity', data=insight_3)
plt.title('Минимальная интенсивность CO2 по регионам')
plt.xlabel('Регион')
plt.ylabel('Интенсивность CO2 (граммы на киловатт-час)')
plt.xticks(rotation=45)
plt.show()


# Визуализация 4: Изменение выбросов CO2 от года к году
plt.figure(figsize=(15, 7))
for country in insight_4['CountryLong'].unique():
    data = insight_4.query(f"CountryLong == '{country}'")
    plt.plot(data['Year'], data['change_in_co2'], label=country)

plt.legend()
plt.xlabel('Год')
plt.ylabel('Изменение выбросов CO2')
plt.title('Изменение выбросов CO2 от года к году по странам')
plt.grid(True)
plt.show()

# Визуализация 5: Динамика изменения выбросов CO2 для Великобритании
plt.figure(figsize=(10,6))
sns.lineplot(x='Year', y='Mean_ValueNumeric_for_United_Kingdom', data=insight_5)
plt.title(f'Изменение средней интенсивности CO2 для {country}')
plt.xlabel('Год')
plt.ylabel('Средняя интенсивность CO2')
plt.show()

# Шаг 4: Загрузка агрегированных данных в базу данных SQLite

# Создаем соединение с базой данных
conn = sqlite3.connect('co2_insights.db')
cursor = conn.cursor()

# Таблица для хранения инсайта 1
cursor.execute('''
CREATE TABLE IF NOT EXISTS insight_1 (
    CountryLong TEXT,
    mean_co2_emissions REAL
)
''')

# Таблица для хранения инсайта 2
cursor.execute('''
CREATE TABLE IF NOT EXISTS insight_2 (
    CountryLong TEXT,
    Year INTEGER,
    TotalCO2Emissions REAL
)
''')

# Таблица для хранения инсайта 2.1
cursor.execute('''
CREATE TABLE IF NOT EXISTS insight_2_1 (
    Year INTEGER PRIMARY KEY,
    Max_CO2_Emissions REAL
)
''')

# Таблица для хранения инсайта 3
cursor.execute('''
CREATE TABLE IF NOT EXISTS insight_3 (
    CountryLong TEXT PRIMARY KEY,
    Min_CO2_Intensity REAL
)
''')

# Таблица для хранения инсайта 4
cursor.execute('''
CREATE TABLE IF NOT EXISTS insight_4 (
    CountryLong TEXT,
    Year INTEGER,
    change_in_co2 REAL
)
''')

# Таблица для хранения инсайта 5
cursor.execute('''
CREATE TABLE IF NOT EXISTS insight_5 (
    Year INTEGER PRIMARY KEY,
    Mean_ValueNumeric_for_United_Kingdom REAL
)
''')

conn.commit()

# Функция для вставки данных в таблицу
def insert_into_table(table_name, data):
    if table_name == 'insight_1':
        cursor.executemany(
            'INSERT OR IGNORE INTO insight_1 (CountryLong, mean_co2_emissions) VALUES (?, ?)',
            data.values
        )
    elif table_name == 'insight_2_1':
        cursor.executemany(
            'INSERT OR REPLACE INTO insight_2_1 (Year, Max_CO2_Emissions) VALUES (?, ?)',
            data.values
        )
    elif table_name == 'insight_2':
        cursor.executemany(
            'INSERT OR IGNORE INTO insight_2 (CountryLong, Year, TotalCO2Emissions) VALUES (?, ?, ?)',
            data.values
        )
    elif table_name == 'insight_3':
        cursor.executemany(
            'INSERT OR REPLACE INTO insight_3 (CountryLong, Min_CO2_Intensity) VALUES (?, ?)',
            data.values
        )
    elif table_name == 'insight_4':
        cursor.executemany(
            'INSERT OR IGNORE INTO insight_4 (CountryLong, Year, change_in_co2) VALUES (?, ?, ?)',
            data.values
        )
    elif table_name == 'insight_5':
        cursor.executemany(
            'INSERT OR REPLACE INTO insight_5 (Year, Mean_ValueNumeric_for_United_Kingdom) VALUES (?, ?)',
            data.values
        )


import sqlite3

# Функция для формирования HTML-отчета
def create_html_table(cursor, table_name):
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()
    
    columns = [description[0] for description in cursor.description]
    
    html = '<html><body><h1>' + table_name + '</h1>'
    html += '<table border="1">'
    html += '<tr>'
    for column in columns:
        html += f'<th>{column}</th>'
    html += '</tr>'
    
    for row in rows:
        html += '<tr>'
        for value in row:
            html += f'<td>{value}</td>'
        html += '</tr>'
        
    html += '</table></body></html>'
    
    return html

if __name__ == "__main__":
    conn = sqlite3.connect('co2_insights.db')
    cursor = conn.cursor()
    
    tables = ['insight_1', 'insight_2', 'insight_2_1', 'insight_3', 'insight_4', 'insight_5']
    
    for table in tables:
        html = create_html_table(cursor, table)
        filename = f'{table}.html'
        with open(filename, 'w') as file:
            file.write(html)
            
    conn.close()








