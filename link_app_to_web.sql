{{
    config(
        materialized = 'table',
        unique_key = 'primary_account_id'
    )
}}
--Notes:
/*
This is a model that links APP users to WEB users using user ids
Created by Jesus Montero

*/

with account_services as s
( -- Gets all the account ids and the  id from the app the first time it was created.
  select 
  distinct
  primary_account_id,
  first_value(h.date_created) over 
    (
              partition by primary_account_id
              order by
                  h.date_created rows between unbounded preceding
                  and unbounded following
    ) as acs_Web_analaytics_tool_reference_date_created, -- the first date the Web_analaytics_tool id was created, without this function the number of records will be in the millions
  primary_reference_number as acs_Web_analaytics_tool_user_id -- Web_analaytics_tool user id from the app
  from account_service_Web_analaytics_tool_id h --table with Web_analaytics_tool user ids
  left join account_service_primary_account a using (primary_account_id) 
  where h.date_created::date >= '2023-01-01' -- limiting the number of records for performance. (Prior to this date there were no app interactions)
), 
Web_analaytics_tool_to_ck as 
( -- Web_analaytics_tool to contact key by primary account. We need the contact key to later join by UTM term.
  select 
  distinct 
  primary_account_id, -- join key 
  acs_Web_analaytics_tool_reference_date_created,--first date from the time the Web_analaytics_tool id was linked to the primary account id. 
  acs_Web_analaytics_tool_user_id, -- SMP Web_analaytics_tool user id
  e.reference_number as acs_email, -- email for that primary_account_id
  reference_number as acs_contact_key,-- contact key (unique identifier for emails or phone numbers)
  first_value(c.date_created) over (
              partition by primary_account_id
              order by
                  c.date_created rows between unbounded preceding
                  and unbounded following
  ) as acs_contact_key_created_date -- date this primary account id got a contact key assigned to it 
  from account_services
  left join account_service_contactkey c using (primary_account_id)
  left join account_service_email e using (primary_account_id)
),
account_to_lku_Web_analaytics_tool as 
( -- Generates the first web user id, joins the previous app Web_analaytics_tool user id and contact key to the most direct app to web linkage we have
  select 
  distinct
  c.primary_account_id,
  acs_Web_analaytics_tool_reference_date_created,
  acs_Web_analaytics_tool_user_id,
  acs_email,
  acs_contact_key,
  acs_contact_key_created_date,
  web_user_id, -- this id comes from the lku_Web_analaytics_tool_app_to_web model, which uses the primary account id stored as a parameter in the url to link web and app user id
  app_user_id,
  first_value(first_session_date) over (
              partition by c.primary_account_id
              order by
                  first_session_date rows between unbounded preceding
                  and unbounded following
  ) as buy_now_user_first_session_date, --this is the first session when the user visited buy now from the app 
  last_value(first_session_date) over (
              partition by c.primary_account_id
              order by
                  first_session_date rows between unbounded preceding
                  and unbounded following
  ) as buy_now_user_last_session_date
  from Web_analaytics_tool_to_ck c
  left join lku_Web_analaytics_tool_app_to_web a 
  on c.acs_Web_analaytics_tool_user_id  = a.app_user_id
  left join fct_Web_analaytics_tool_sessions f on f.user_id = a.web_user_id
),
--------------------This CTE uses the contact key saved as utm term for all the comunications we send. We get the CC of the app user and the utm term from web sessions derived from comunications send to the user
pageviews_term as 
(
  select 
  distinct
  p.user_id::varchar as web_user_id_from_pageviews,
  p.utm_term,
  f.first_session_date as contact_key_user_first_session
  from Web_analaytics_tool_page_views p
  left join fct_Web_analaytics_tool_sessions f on f.user_id = p.user_id
  where p.utm_source ='sfmc' and p.utm_term is not null and p.session_time::date >'2022-01-01' --the utm term having the Ck is exclusive to CRM coms. Session is limited for performance.
),

------------------This is the final CTE, it uses the primary account id parameter stored in the UTM of each custom smile plan view.
page_views_for_account_id_directly as 
(
  select  distinct
  p.user_id::varchar as account_id_Web_analaytics_tool_user,
  s.first_session_date as account_id_Web_analaytics_tool_user_first_session,
  case
      when regexp_instr(query, 'accountId=[^&%]*') > 0 then --This regex function extracts the account id from the URL on the app. 
        case
          when position('&', query, position('accountId=', query)) > 0 then
            substring(query, position('accountId=', query) + length('accountId='), position('&', query, position('accountId=', query)) - position('accountId=', query) - length('accountId='))
          when position('%', query, position('accountId=', query)) > 0 then
            substring(query, position('accountId=', query) + length('accountId='), position('%', query, position('accountId=', query)) - position('accountId=', query) - length('accountId='))
          else
            substring(query, position('accountId=', query) + length('accountId='))
        end
      else null -- or any default value if accountid doesn't exist in the field
    end as primary_account_id
  from Web_analaytics_tool_page_views p
  left join fct_Web_analaytics_tool_sessions s on s.user_id = p.user_id
  where path like '%/custom-smile-plan-3d%'
),
final1 as 
(
  select * from account_to_lku_Web_analaytics_tool h
  left join pageviews_term p 
  on h.acs_contact_key = p.utm_term
),
final2 as 
(
  select * from final1
  left join page_views_for_account_id_directly using (primary_account_id)
)
select  -- final naming changes
primary_account_id,
acs_Web_analaytics_tool_reference_date_created,
acs_Web_analaytics_tool_user_id,
acs_email,
acs_contact_key,
acs_contact_key_created_date,
web_user_id as buy_now_user_id,
buy_now_user_first_session_date,
web_user_id_from_pageviews as contact_key_user_id,
contact_key_user_first_session,
account_id_Web_analaytics_tool_user,
account_id_Web_analaytics_tool_user_first_session
from final2

