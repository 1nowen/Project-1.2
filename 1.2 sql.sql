
----СОЗДАНИЕ ТАБЛИЦ----
-- CREATE SCHEMA IF NOT EXISTS DM;

-- Таблица оборотов
-- CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_TURNOVER_F (
--     on_date DATE NOT NULL,
--     account_rk INTEGER NOT NULL,
--     credit_amount NUMERIC(23,8),
--     credit_amount_rub NUMERIC(23,8),
--     debet_amount NUMERIC(23,8),
--     debet_amount_rub NUMERIC(23,8),
--     PRIMARY KEY (on_date, account_rk)
-- );

-- -- Таблица остатков
-- CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_BALANCE_F (
--     on_date DATE NOT NULL,
--     account_rk INTEGER NOT NULL,
--     balance_out NUMERIC(23,8),
--     balance_out_rub NUMERIC(23,8),
--     PRIMARY KEY (on_date, account_rk)
-- );
-- ALTER TABLE logs.load_log ADD column on_date DATE;
--------------------------------------------------------------------------------------
----ПРОЦЕДУРА РАСЧЕТА ОБОРОТОВ----
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_id INTEGER;
    rows_affected INTEGER;
	v_start_time timestamp;
BEGIN
    -- Логирование начала операции
	v_start_time := clock_timestamp();
    INSERT INTO LOGS.load_log(table_name, start_time, status, on_date) 
    VALUES ('dm_account_turnover_f', v_start_time, 'started', i_OnDate)
    RETURNING id INTO log_id;
    
    -- Удаление существующих данных за дату
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;
    
    -- Вставка новых данных
    INSERT INTO dm.dm_account_turnover_f
    SELECT
        i_OnDate AS on_date,
        acc.account_rk,
        COALESCE(SUM(p.credit_amount), 0) AS credit_amount,
        COALESCE(SUM(p.credit_amount * COALESCE(er.reduced_course, 1)), 0) AS credit_amount_rub,
        COALESCE(SUM(p.debet_amount), 0) AS debet_amount,
        COALESCE(SUM(p.debet_amount * COALESCE(er.reduced_course, 1)), 0) AS debet_amount_rub
    FROM (SELECT DISTINCT account_rk FROM ds.md_account_d WHERE i_OnDate BETWEEN data_actual_date AND data_actual_end_date) acc
    LEFT JOIN ds.ft_posting_f p 
        ON (p.credit_account_rk = acc.account_rk OR p.debet_account_rk = acc.account_rk)
        AND p.oper_date = i_OnDate
    LEFT JOIN ds.md_exchange_rate_d er 
        ON er.currency_rk = (SELECT currency_rk FROM ds.md_account_d 
                            WHERE account_rk = acc.account_rk 
                            AND i_OnDate BETWEEN data_actual_date AND data_actual_end_date)
        AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
    GROUP BY acc.account_rk
    HAVING COALESCE(SUM(p.credit_amount), 0) != 0 OR COALESCE(SUM(p.debet_amount), 0) != 0;
    
    -- Обновление лога
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    UPDATE LOGS.load_log 
    SET 
		end_time = clock_timestamp(),
        status = 'completed',
        rows_loaded = rows_affected
    WHERE id = log_id;
    
EXCEPTION WHEN OTHERS THEN
    UPDATE LOGS.load_log 
    SET 
        end_time = clock_timestamp(),
        status = 'error',
        error_message = SQLERRM
    WHERE id = log_id;
    RAISE;
END;
$$;
------------------------------------------------------------------------------------------------
----Остатки на 31.12.2017----
-- Удаляю возможные существующие данные за 31.12.2017
DELETE FROM dm.dm_account_balance_f WHERE on_date = '2017-12-31';

INSERT INTO dm.dm_account_balance_f
SELECT 
    b.on_date,
    b.account_rk,
    b.balance_out,
    b.balance_out * COALESCE(er.reduced_course, 1) AS balance_out_rub
FROM DS.FT_BALANCE_F b
LEFT JOIN ds.md_account_d a 
    ON b.account_rk = a.account_rk
    AND b.on_date BETWEEN a.data_actual_date AND a.data_actual_end_date
LEFT JOIN ds.md_exchange_rate_d er 
    ON er.currency_rk = a.currency_rk
    AND b.on_date BETWEEN er.data_actual_date AND er.data_actual_end_date
WHERE b.on_date = '2017-12-31';	
----------------------------------------------------------------------------------------
----ПРОЦЕДУРА ДЛЯ ВИТРИНЫ ОСТАТКОВ----
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    log_id INTEGER;
    rows_affected INTEGER;
	v_start_time timestamp;
BEGIN
    -- Логирование начала
	v_start_time := clock_timestamp();
    INSERT INTO LOGS.load_log(table_name, start_time, status, on_date) 
    VALUES ('DM_ACCOUNT_BALANCE_F', v_start_time, 'started', i_OnDate)
    RETURNING id INTO log_id;
    
    -- Удаление старых данных
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;
    
    -- Расчет новых остатков
    INSERT INTO dm.dm_account_balance_f
    SELECT
        i_OnDate,
        a.account_rk,
        CASE a.char_type
            WHEN 'А' THEN COALESCE(prev.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN 'П' THEN COALESCE(prev.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
        CASE a.char_type
            WHEN 'А' THEN COALESCE(prev.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN 'П' THEN COALESCE(prev.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM ds.md_account_d a
    LEFT JOIN dm.dm_account_balance_f prev 
        ON prev.account_rk = a.account_rk 
        AND prev.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN dm.dm_account_turnover_f t 
        ON t.account_rk = a.account_rk 
        AND t.on_date = i_OnDate
    WHERE i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date;
    
    -- Обновление лога
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
	UPDATE LOGS.load_log 
		SET 
			end_time = clock_timestamp(),
			status = 'completed',
			rows_loaded = rows_affected
		WHERE id = log_id;
	
EXCEPTION WHEN OTHERS THEN
    UPDATE LOGS.load_log 
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
-- SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = '2017-12-31';
-- SELECT * FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = '2017-12-31';

-- ----Тест оборотов в разные дни----
-- -- Обороты за 5 января
-- SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F 
-- WHERE on_date = '2018-01-5';
-- Обороты за 14 января
-- SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F 
-- WHERE on_date = '2018-01-14';
-- Обороты за 19 января
-- SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F 
-- WHERE on_date = '2018-01-19';

----Тест остатков в разные дни----
-- -- Остатки за 5 января
-- SELECT * FROM DM.DM_ACCOUNT_BALANCE_F 
-- WHERE on_date = '2018-01-05';
-- -- Остатки за 14 января
-- SELECT * FROM DM.DM_ACCOUNT_BALANCE_F 
-- WHERE on_date = '2018-01-14';
-- -- Остатки за 31 января
-- SELECT * FROM DM.DM_ACCOUNT_BALANCE_F 
-- WHERE on_date = '2018-01-31';


-- TRUNCATE logs.load_log;
-- TRUNCATE dm.dm_account_turnover_f;
-- TRUNCATE dm.dm_account_balance_f;