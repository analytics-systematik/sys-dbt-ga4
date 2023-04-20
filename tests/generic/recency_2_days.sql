{% test recency_2_days(model, column_name) %}

    SELECT
        count(*) as num_rows
    FROM {{ model }}
    WHERE DATE( {{column_name}} ) >= DATE_SUB(current_date, INTERVAL 2 DAY)
    HAVING NOT num_rows > 0

{% endtest %}