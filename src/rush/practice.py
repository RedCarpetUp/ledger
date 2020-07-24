                    WITH RECURSIVE date_wise_lender_payable_balance AS (
                        SELECT DATE(ledger_trigger_event.post_date) AS post_date, COALESCE(get_account_balance(19,'card', 'lender_payable', 'l', DATE(ledger_trigger_event.post_date)), 0) AS amount FROM ledger_trigger_event
                        WHERE ledger_trigger_event.post_date > CAST('2020-03-01' AS DATE) AND ledger_trigger_event.post_date <= CAST('2020-06-01' AS DATE)
                        GROUP BY DATE(ledger_trigger_event.post_date) ORDER BY DATE(ledger_trigger_event.post_date) DESC
                    ),
                    day_wise_lender_payable_balance AS (
                        SELECT (post_date - CAST('2020-03-01' AS DATE)) AS days, amount FROM date_wise_lender_payable_balance WHERE amount > 0
                    ),
                    interest_on_lender_payable_balance AS (
                        SELECT (days - 1 - COALESCE(LAG(days) OVER(ORDER BY days), 0)) AS daily, amount FROM day_wise_lender_payable_balance
                    ),
                    reccruring_interest_on_lender_payable_balance_view AS (
                        SELECT *, POW(1.0004931506849, daily)* amount - amount as interest_amount, row_number() over(order by amount) as rn FROM interest_on_lender_payable_balance order by daily desc 
                    ),
                    rec_query(rn, days, amount, amount_interest) as 
                    (
                        select rn, CAST(daily AS NUMERIC), amount, interest_amount
                        from reccruring_interest_on_lender_payable_balance_view
                        where rn = 1
                        union all   
                        select 
                        t.rn, t.daily, t.amount, POW(1.0004931506849, t.daily)*(t.amount + p.amount_interest)
                        from rec_query p
                        join reccruring_interest_on_lender_payable_balance_view t on t.rn = p.rn + 1
                    )
                    SELECT ROUND(amount_interest, 2) FROM rec_query order by rn desc limit 1;