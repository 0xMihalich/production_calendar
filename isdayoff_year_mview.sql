/*
    Материализованное представление производственного календаря для Clickhouse на текущий год
    Проверка даты на принадлежность к нерабочему дню, согласно официальным указам и распоряжениям
    Основано на данных из API isDayOff() https://www.isdayoff.ru/
    В качестве тригера может быть использована любая таблица,
    содержащая не менее одной колонки с типом данных Date
    В текущем примере была использована колонка current_period из таблицы TABLE.schedule
    Для получения данных на следующий год в конце текущего года в запросе использована
    функция toIntervalMonth со значением 1
    Если необходимо получить данные на следующий год раньше декабря, измените значение
    с 1 на необходимый интервал в строках 45 и 85
*/

-- TABLE.production_calendar source
CREATE MATERIALIZED VIEW TABLE.production_calendar
(
    `start_month`    Date            COMMENT 'Начало месяца',
    `month_day`      Date            COMMENT 'День месяца',
    `week_number`    Int32           COMMENT 'Номер дня недели',
    `week_name`      FixedString(22) COMMENT 'День недели',
    `week_short`     FixedString(4)  COMMENT 'Сокращение дня недели',
    `status_number`  Int32           COMMENT 'Статус дня (рабочий/нерабочий/сокращенный)',
    `status_decode`  FixedString(46) COMMENT 'Расшифровка статуса',
    `quarter_number` Int32           COMMENT 'Номер квартала',
    `year_week`      Int32           COMMENT 'Номер недели в году'
)
ENGINE = MergeTree
ORDER BY (start_month, month_day)
SETTINGS index_granularity = 8192
POPULATE AS
SELECT DISTINCT
    start_month,
    month_day,
    week_number,
    weekdays[1] AS week_name,
    weekdays[2] AS week_short,
    status_number,
    status_decode,
    quarter_number,
    year_week
FROM TABLE.schedule AS s
FULL OUTER JOIN
(
    SELECT
        toStartOfYear(now() + toIntervalMonth(1)) AS first_day,
        groupArray(*) AS mark,
        arrayJoin(arrayEnumerate(mark)) AS d,
        first_day + toIntervalDay(d - 1) AS month_day,
        toStartOfMonth(month_day) AS start_month,
        toDayOfWeek(month_day) AS week_number,
        multiIf(week_number = 1,
                    ['Понедельник', 'ПН'],
                week_number = 2,
                    ['Вторник', 'ВТ'],
                week_number = 3,
                    ['Среда', 'СР'],
                week_number = 4,
                    ['Четверг', 'ЧТ'],
                week_number = 5,
                    ['Пятница', 'ПТ'],
                week_number = 6,
                    ['Суббота', 'СБ'],
                week_number = 7,
                    ['Воскресение', 'ВС'],
                [NULL]) AS weekdays,
        mark[d] AS status_number,
        multiIf(status_number = 0,
                    'Рабочий день',
                status_number = 1,
                    'Нерабочий день',
                status_number = 2,
                    'Сокращённый рабочий день',
                status_number = 4,
                    'Рабочий день',
                status_number = 100,
                    'Ошибка в дате',
                status_number = 101,
                    'Данные не найдены',
                status_number = 199,
                    'Ошибка сервиса',
                NULL) AS status_decode,
        toQuarter(month_day) AS quarter_number,
        toWeek(month_day) AS year_week
    FROM url(concat('https://isdayoff.ru/api/getdata?year=',
                    formatDateTime(now() + toIntervalMonth(1), '%Y'),
                    '&cc=ru&pre=1&delimeter=%0A&covid=1&sd=0'),
                    'CSV')
) AS calendar ON s.current_period = calendar.start_month
WHERE status_decode IS NOT NULL
ORDER BY
    start_month ASC,
    month_day ASC;
