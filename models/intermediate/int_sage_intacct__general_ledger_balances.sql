with general_ledger as (
    select *
    from {{ ref('sage_intacct__general_ledger') }}
), 

gl_accounting_periods as (
    select *
    from {{ ref('int_sage_intacct__general_ledger_date_spine') }}
), 


gl_period_balances_is as (
    select 
        location_id,
        location_name,
        account_no,
        account_title,
        book_id,
        category,
        classification,
        currency, 
        entry_state,
        account_type,
        cast({{ dbt.date_trunc("month", "entry_date_at") }} as date) as date_month, 
        cast({{ dbt.date_trunc("year", "entry_date_at") }} as date) as date_year

        {% if var('sage_account_pass_through_columns') %} 
        , 
        {{ var('sage_account_pass_through_columns') | join (", ")}}

        {% endif %}
        , 
        sum(amount) as period_amount
    from general_ledger
    where account_type = 'incomestatement'
    
    {{ dbt_utils.group_by(12 + var('sage_account_pass_through_columns')|length) }}

), 

gl_period_balances_bs as (
    select 
        location_id,
        location_name,
        account_no,
        account_title,
        book_id,
        category,
        classification,
        currency,
        entry_state,
        account_type,
        cast({{ dbt.date_trunc("month", "entry_date_at") }} as date) as date_month, 
        cast({{ dbt.date_trunc("year", "entry_date_at") }} as date) as date_year

        {% if var('sage_account_pass_through_columns') %} 
        , 
        {{ var('sage_account_pass_through_columns') | join (", ")}}

        {% endif %}
        ,
        sum(amount) as period_amount
    from general_ledger
    where account_type = 'balancesheet'
    
    {{ dbt_utils.group_by(12 + var('sage_account_pass_through_columns')|length) }}

), 

gl_period_balances as (
    select *
    from gl_period_balances_bs

    union all

    select *
    from gl_period_balances_is

),

gl_cumulative_balances as (
    select 
        *,
        case
            when account_type = 'balancesheet' then sum(period_amount) over (partition by location_id, location_name, account_no, account_title, book_id, entry_state 
                {% if var('sage_account_pass_through_columns') %} 
                , 
                {{ var('sage_account_pass_through_columns') | join (", ")}}

                {% endif %}

                order by date_month, location_id, account_no rows unbounded preceding)
            else 0 
        end as cumulative_amount   
    from gl_period_balances

), 

gl_beginning_balance as (
    select 
        *,
        case
            when account_type = 'balancesheet' then (cumulative_amount - period_amount) 
            else 0 
        end as period_beg_amount,
        period_amount as period_net_amount, 
        cumulative_amount as period_ending_amount
    from gl_cumulative_balances

), 

gl_patch as (
    select 
        coalesce(gl_beginning_balance.location_id, gl_accounting_periods.location_id) as location_id,
        coalesce(gl_beginning_balance.location_name, gl_accounting_periods.location_name) as location_name,
        coalesce(gl_beginning_balance.account_no, gl_accounting_periods.account_no) as account_no,
        coalesce(gl_beginning_balance.account_title, gl_accounting_periods.account_title) as account_title,
        coalesce(gl_beginning_balance.book_id, gl_accounting_periods.book_id) as book_id,
        coalesce(gl_beginning_balance.category, gl_accounting_periods.category) as category,
        coalesce(gl_beginning_balance.classification, gl_accounting_periods.classification) as classification,
        coalesce(gl_beginning_balance.currency, gl_accounting_periods.currency) as currency,
        coalesce(gl_beginning_balance.entry_state, gl_accounting_periods.entry_state) as entry_state,
        coalesce(gl_beginning_balance.account_type, gl_accounting_periods.account_type) as account_type,
        coalesce(gl_beginning_balance.date_year, gl_accounting_periods.date_year) as date_year

        {% if var('sage_account_pass_through_columns') %} 
        , 
        {{ var('sage_account_pass_through_columns') | join (", gl_beginning_balance.")}}

        {% endif %}
        ,
        gl_accounting_periods.period_first_day,
        gl_accounting_periods.period_last_day,
        gl_accounting_periods.period_index,
        gl_beginning_balance.period_net_amount,
        gl_beginning_balance.period_beg_amount,
        gl_beginning_balance.period_ending_amount,
        case 
            when gl_beginning_balance.period_beg_amount is null and period_index = 1 then 0
            else gl_beginning_balance.period_beg_amount
        end as period_beg_amount_starter,
        case
            when gl_beginning_balance.period_ending_amount is null and period_index = 1 then 0
            else gl_beginning_balance.period_ending_amount
        end as period_ending_amount_starter
    from gl_accounting_periods

    left join gl_beginning_balance
        on gl_beginning_balance.location_id = gl_accounting_periods.location_id
            and gl_beginning_balance.location_name = gl_accounting_periods.location_name
            and gl_beginning_balance.account_no = gl_accounting_periods.account_no
            and gl_beginning_balance.account_title = gl_accounting_periods.account_title
            and gl_beginning_balance.date_month = gl_accounting_periods.period_first_day
            and gl_beginning_balance.book_id = gl_accounting_periods.book_id
            and gl_beginning_balance.entry_state = gl_accounting_periods.entry_state
            and gl_beginning_balance.currency = gl_accounting_periods.currency

), 

gl_value_partition as (
    select
        *,
        sum(case when period_ending_amount_starter is null then 0 else 1 end) over (order by location_id, location_name, account_no, account_title, book_id, entry_state, period_last_day rows unbounded preceding) as gl_partition
    from gl_patch

), 

final as (
    select
        location_id,
        location_name,
        account_no,
        account_title,
        book_id,
        category,
        classification,
        currency,
        account_type,
        date_year, 
        entry_state,
        period_first_day,
        period_last_day,
        coalesce(period_net_amount,0) as period_net_amount,
        coalesce(period_beg_amount_starter,
            first_value(period_ending_amount_starter) over (partition by gl_partition order by period_last_day rows unbounded preceding)) as period_beg_amount,
        coalesce(period_ending_amount_starter,
            first_value(period_ending_amount_starter) over (partition by gl_partition order by period_last_day rows unbounded preceding)) as period_ending_amount
        {% if var('sage_account_pass_through_columns') %} 
        , 
        {{ var('sage_account_pass_through_columns') | join (", ")}}

        {% endif %}
        
    from gl_value_partition
)

select *
from final
