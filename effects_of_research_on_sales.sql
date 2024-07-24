/*
This is a simple script to understand the impact of a team research presentation on the opportunities for specific companies.
*/

WITH accounts AS ( -- This CTE captures the accounts from the main CRM table 
    SELECT 
        main_company_identifier,
        account_id,
        account_name,
        account_type
    FROM
        dim_crm_accounts
), 
opps AS ( -- Accounts can have multiple opportunities, this CTE captures those opportunities
    SELECT 
        account_id,
        name,
        opportunity_id,
        stage_name,
        amount,
        close_date::date AS opp_close_date,
        type,
        is_opp_closed,
        is_opp_won,
        created_date::date AS opp_created_date,
        last_modified_date::date AS opp_last_modified_date,
        reason_lost_c,
        closed_won_time_stamp_c::date AS opp_closed_won_date,
        total_possible_value,
        opp_owner_name,
        gross_new_value
    FROM 
        dim_crm_opps
),
oral_presentation_log AS ( -- This is a simple table with records on when the research was delivered and presented
    SELECT 
        main_company_identifier,
        deck_delivered_date,
        deck_presented_date
    FROM 
        research_log 
),
unioned AS (
    SELECT * 
    FROM accounts
    LEFT JOIN opps USING (account_id)
    LEFT JOIN oral_presentation_log USING (main_company_identifier)
),
final AS ( -- This CTE transforms the opportunity-level data into company-level data, creating multiple columns based on different conditions
    SELECT
        DISTINCT main_company_identifier,
        SUM(CASE WHEN opp_close_date >= deck_presented_date THEN 1 ELSE 0 END) AS total_opps_closed_after_deck_delivered,
        SUM(CASE WHEN opp_close_date >= deck_presented_date AND is_opp_won = TRUE THEN 1 ELSE 0 END) AS total_opps_won_after_deck_delivered,
        SUM(CASE WHEN opp_close_date >= deck_presented_date AND is_opp_won = FALSE THEN 1 ELSE 0 END) AS total_opps_lost_after_deck_delivered,
        SUM(CASE WHEN opp_close_date >= deck_presented_date AND is_opp_won = TRUE AND type = 'Renewal' THEN 1 ELSE 0 END) AS total_renewal_won_after_deck_delivered,
        SUM(CASE WHEN opp_close_date >= deck_presented_date AND is_opp_won = TRUE AND (type = 'New' OR type = 'New Logo') THEN 1 ELSE 0 END) AS total_new_opps_won_after_deck_delivered
    FROM 
        unioned
    GROUP BY 
        main_company_identifier
    ORDER BY 
        total_opps_closed_after_deck_delivered DESC
)
SELECT * 
FROM final;
