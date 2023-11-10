with general_ledger_by_period as (
    select *
    from {{ ref('sage_intacct__general_ledger_by_period') }}
),

retained_earnings_prep as (
    select
        period_first_day,
        location_id,
        location_name,
        'dbt Package Generated' as account_no,
        'Adj. Net Income' as account_title,
        'balancesheet' as account_type,
        book_id,
        'Retained Earnings' as category,
        'Equity' as classification,
        currency,
        entry_state,
        sum(period_net_amount) as period_net_amount
    from general_ledger_by_period
    where account_type = 'incomestatement'
    group by period_first_day, location_id, location_name, book_id, entry_state, currency
),

final as (
    select
        period_first_day,
        location_id,
        location_name,
        account_no,
        account_title,
        account_type,
        book_id,
        category,
        classification,
        currency,
        entry_state,
        sum(period_net_amount) over (partition by location_id, location_name, book_id, entry_state, currency
            order by period_first_day, location_id, location_name rows between unbounded preceding and current row
        ) as amount
    from retained_earnings_prep
)

select *
from final
