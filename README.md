# ETL project

Данный проект является ETL-проектом. Сначала загружаем csv файл из облачного хранилища, затем обрабатываем, делаем аналитику и результат загружаем в базу данных ClickHouse.

## Инструкция по запуску:
### Собираем Docker-образы
Для этого в терминале пишем
```bash
docker-compose up -d --build
```
## Сервисы
| Сервис     | Адрес                 |
|------------|-----------------------|
| Airflow | http://localhost:8080 |
|      ClickHouse        |   http://localhost:9000                    |

## В этом проекте были поставлены задачи:
1. Загрузите файл данных в DataFrame PySpark. Обязательно выведите количество строк.

2. Убедитесь, что данные корректно прочитаны (правильный формат, отсутствие пустых строк).

3. Преобразуйте текстовые и числовые поля в соответствующие типы данных (например, дата, число).

4. Вычислите средний и медианный год постройки зданий.

5. Определите топ-10 областей и городов с наибольшим количеством объектов.

6. Найдите здания с максимальной и минимальной площадью в рамках каждой области.

7. Определите количество зданий по десятилетиям (например, сколько зданий построено в 1950-х, 1960-х и т.д.).

8. Создайте схему таблицы в ClickHouse, которая будет соответствовать структуре ваших данных. Это можно сделать не через airflow.

9. Настройте соединение с ClickHouse из скрипта, учтите, что сделать это необходимо в airflow.

10. Загрузите обработанные данные из DataFrame в таблицу в ClickHouse (airflow)

11. Выполните SQL скрипт в Python, который выведет топ 25 домов, у которых площадь больше 60 кв.м (airflow)

# Для этого использовались:
1. Docker-compose c apache/airflow:2.9.2 с сервисом ClickHouse

2. Dockerfile, где добавил к airflow зависимости и требуемые библиотеки, которые прописаны в requirements.txt

3. В файле main.py прописан код выполнения проекта и все это обернуто в DAG.
После запуска docker-compose up, необходимо переместить файл main.py в папку dags

# Файл, используемый в проекте для обработки:

https://disk.yandex.ru/d/bhf2M8C557AFVw