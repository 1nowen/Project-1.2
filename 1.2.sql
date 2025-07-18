
----СОЗДАНИЕ ТАБЛИЦ----
-- CREATE SCHEMA IF NOT EXISTS DM;

-- Таблица оборотов
CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_TURNOVER_F (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);

-- Таблица остатков
CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);
-- ALTER TABLE logs.load_log ADD column on_date DATE;
--------------------------------------------------------------------------------------
----ПРОЦЕДУРА РАСЧЕТА ОБОРОТОВ----
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_id INTEGER;
    rows_affected INTEGER;
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    INSERT INTO logs.load_log(table_name, start_time, status, on_date) 
    VALUES ('dm_account_turnover_f', v_start_time, 'started', i_OnDate)
    RETURNING id INTO log_id;

    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    WITH all_accounts AS (
        SELECT credit_account_rk AS account_rk FROM ds.ft_posting_f WHERE oper_date = i_OnDate
        UNION 
        SELECT debet_account_rk FROM ds.ft_posting_f WHERE oper_date = i_OnDate
    ),
    credit_turns AS (
        SELECT credit_account_rk AS account_rk, SUM(credit_amount) AS credit_amount
        FROM ds.ft_posting_f WHERE oper_date = i_OnDate GROUP BY credit_account_rk
    ),
    debet_turns AS (
        SELECT debet_account_rk AS account_rk, SUM(debet_amount) AS debet_amount
        FROM ds.ft_posting_f WHERE oper_date = i_OnDate GROUP BY debet_account_rk
    ),
    account_currency AS (
        SELECT a.account_rk, md.currency_rk
        FROM all_accounts a
        JOIN ds.md_account_d md ON a.account_rk = md.account_rk 
            AND i_OnDate BETWEEN md.data_actual_date AND COALESCE(md.data_actual_end_date, '9999-12-31')
    ),
    exchange_rates AS (
        SELECT ac.account_rk,
            (SELECT er.reduced_course 
             FROM ds.md_exchange_rate_d er
             WHERE er.currency_rk = ac.currency_rk
                 AND er.data_actual_date <= i_OnDate
             ORDER BY er.data_actual_date DESC 
             LIMIT 1) AS reduced_course
        FROM account_currency ac
    )
    INSERT INTO dm.dm_account_turnover_f
    SELECT
        i_OnDate,
        a.account_rk,
        COALESCE(ct.credit_amount, 0),
        COALESCE(ct.credit_amount, 0) * COALESCE(er.reduced_course, 1),
        COALESCE(dt.debet_amount, 0),
        COALESCE(dt.debet_amount, 0) * COALESCE(er.reduced_course, 1)
    FROM all_accounts a
    LEFT JOIN credit_turns ct ON a.account_rk = ct.account_rk
    LEFT JOIN debet_turns dt ON a.account_rk = dt.account_rk
    LEFT JOIN exchange_rates er ON a.account_rk = er.account_rk
    WHERE COALESCE(ct.credit_amount, 0) != 0 OR COALESCE(dt.debet_amount, 0) != 0;

    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    UPDATE logs.load_log SET end_time = clock_timestamp(), status = 'completed', rows_loaded = rows_affected
    WHERE id = log_id;

EXCEPTION WHEN OTHERS THEN
    UPDATE logs.load_log SET end_time = clock_timestamp(), status = 'error', error_message = SQLERRM
    WHERE id = log_id;
    RAISE;
END;
$$;

------------------------------------------------------------------------------------------------
--Остатки на 31.12.2017----
-- Удаляю возможные существующие данные за 31.12.2017
DELETE FROM dm.dm_account_balance_f WHERE on_date = '2017-12-31';

------------------------------------------------

-- Заполняем витрину DM.DM_ACCOUNT_BALANCE_F за '31.12.2017'

INSERT INTO dm.dm_account_balance_f
SELECT 
    b.on_date,  
    b.account_rk, 
    b.balance_out, 
    b.balance_out * COALESCE(er.reduced_course, 1) AS balance_out_rub
FROM ds.ft_balance_f b
LEFT JOIN ds.md_exchange_rate_d er 
    ON b.currency_rk = er.currency_rk
    AND '2017-12-31' BETWEEN er.data_actual_date AND er.data_actual_end_date;


-- ----------------------------------------------------------------------------------------
-- ----ПРОЦЕДУРА ДЛЯ ВИТРИНЫ ОСТАТКОВ----
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_id INTEGER;
    rows_affected INTEGER;
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Логирование начала операции
    INSERT INTO logs.load_log(table_name, start_time, status, on_date) 
    VALUES ('dm_account_balance_f', v_start_time, 'started', i_OnDate)
    RETURNING id INTO log_id;

    -- Удаление существующих записей за дату расчета
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    -- Расчет новых остатков
    INSERT INTO dm.dm_account_balance_f
    SELECT
        i_OnDate,
        a.account_rk,
        -- Расчет balance_out в валюте
        CASE a.char_type
            WHEN 'А' THEN 
                COALESCE(prev.balance_out, 0) 
                + COALESCE(ct.debet_amount, 0) 
                - COALESCE(ct.credit_amount, 0)
            WHEN 'П' THEN 
                COALESCE(prev.balance_out, 0) 
                - COALESCE(ct.debet_amount, 0) 
                + COALESCE(ct.credit_amount, 0)
        END AS balance_out,
        -- Расчет balance_out_rub
        CASE 
            WHEN a.char_type = 'А' THEN 
                COALESCE((
                    SELECT balance_out_rub
                    FROM dm.dm_account_balance_f 
                    WHERE account_rk = a.account_rk 
                    AND on_date = i_OnDate - INTERVAL '1 day'
                ), 0)
                + COALESCE((
                    SELECT SUM(debet_amount_rub)
                    FROM dm.dm_account_turnover_f 
                    WHERE account_rk = a.account_rk 
                    AND on_date = i_OnDate
                ), 0)
                - COALESCE((
                    SELECT SUM(credit_amount_rub)
                    FROM dm.dm_account_turnover_f 
                    WHERE account_rk = a.account_rk 
                    AND on_date = i_OnDate
                ), 0)
            WHEN a.char_type = 'П' THEN 
                COALESCE((
                    SELECT balance_out_rub
                    FROM dm.dm_account_balance_f 
                    WHERE account_rk = a.account_rk 
                    AND on_date = i_OnDate - INTERVAL '1 day'
                ), 0)
                - COALESCE((
                    SELECT SUM(debet_amount_rub)
                    FROM dm.dm_account_turnover_f 
                    WHERE account_rk = a.account_rk 
                    AND on_date = i_OnDate
                ), 0)
                + COALESCE((
                    SELECT SUM(credit_amount_rub)
                    FROM dm.dm_account_turnover_f 
                    WHERE account_rk = a.account_rk 
                    AND on_date = i_OnDate
                ), 0)
        END AS balance_out_rub
    FROM ds.md_account_d a
    LEFT JOIN dm.dm_account_balance_f prev 
        ON prev.account_rk = a.account_rk 
        AND prev.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN dm.dm_account_turnover_f ct 
        ON ct.account_rk = a.account_rk 
        AND ct.on_date = i_OnDate
    WHERE i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date;

    -- Логирование успешного завершения
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    UPDATE logs.load_log 
    SET 
        end_time = clock_timestamp(),
        status = 'completed',
        rows_loaded = rows_affected
    WHERE id = log_id;

EXCEPTION WHEN OTHERS THEN
    -- Логирование ошибки
    UPDATE logs.load_log 
    SET 
        end_time = clock_timestamp(),
        status = 'error',
        error_message = SQLERRM
    WHERE id = log_id;
    RAISE;
END;
$$;
------------------------------------------------------------------------------------
----Расчет за 01.2018----
DO $$
DECLARE 
    calc_date DATE := '2018-01-01';
BEGIN
    WHILE calc_date <= '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(calc_date);
        CALL ds.fill_account_balance_f(calc_date);
        calc_date := calc_date + INTERVAL '1 day';
    END LOOP;
END $$;
------------------------------------------------------------------------------------
----ТЕСТЫ----
-- SELECT * FROM DM.DM_ACCOUNT_BALANCE_F
-- ORDER BY on_date, account_rk;
-- SELECT * FROM dm.dm_account_balance_f
-- WHERE account_rk = 13631
-- ORDER BY on_date ASC, account_rk ASC;
