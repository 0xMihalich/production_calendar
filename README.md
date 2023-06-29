# production_calendar
## Материализованное представление производственного календаря для Clickhouse (SQL)

[Скриншот](https://github.com/0xMihalich/production_calendar/blob/main/production_calendar.jpg?raw=true)

Проверка даты на принадлежность к нерабочему дню, согласно официальным указам и распоряжениям

Основано на данных из API isDayOff() https://www.isdayoff.ru/

В качестве тригера может быть использована любая таблица,

содержащая не менее одной колонки с типом данных Date

В текущем примере была использована колонка current_period из таблицы TABLE.schedule

Для получения данных на следующий год в конце текущего года в запросе использована

функция toIntervalMonth со значением 1

Если необходимо получить данные на следующий год раньше декабря, измените значение

с 1 на необходимый интервал в строках 45 и 85


Так же добавлен код для создания обычного представления календаря рабочих дней на 1 месяц


Для чего я это написал? Было интересно поподробнее изучить функции для работы с датами,

Array и редко используемую функцию url, открывающую возможности парсинга сайтов,

работы с различными API и многое другое внутри базы Clickhouse


Кому может быть полезна данная таблица? Возможно, многим=)
