{{
    config(
        materialized = 'incremental',
        unique_key = 'distinct_session_id',
    )
}}
--Notes: This model combined elements from sessions, events and paid media tables to generate a session level table with attribution and wheter or not key events happened.
-- Created by Jesus Montero


WITH stg_sessions as ( -- get all sessions from FCT web_analaytics_tool sessions

    select * from {{ref('fct_web_analaytics_tool_sessions')}}

    where grouped_landing_pages != 'App sessions'

    {% if is_incremental() %}
        and session_start_time > (select  dateadd(day, -1, max("DATE")) from {{ this }})
    {% endif %}
    

), stg_events as ( -- get all events from web_analaytics_tool events

    select * from {{ref('web_analaytics_tool_events')}}

    where 1=1

    {% if is_incremental() %}
        and time > (select  dateadd(day, -1, max("DATE")) from {{ this }})
    {% endif %}

), utm_def  AS (

    -- To capture histoical rtg/prsp from when not properly tagged
    SELECT
        s.utm_campaign as ad_campaign,
        CASE
            WHEN LOWER(s.utm_campaign) IN ('dceml','dcrtg','igdcrtg','ignycrtg','nonop','nonop516','nonop616',
                'nyceml','nycrtg','rtg','rtg-dsk','rtg-nonop','retargeting','retargeting_new_pixel','retargeting_new_pixel',
                'blog41916','draymond-game-7','retargeting_new','dc-rtg','nyc-rtg','nycnonop','dcnonop','nshnonop','pagevisit',
                'otpagevisit','bfeb','blackfriday','cybermonday')
            THEN 'retargeting' ELSE 'prospecting' END AS ad_intention

    FROM (SELECT DISTINCT utm_campaign FROM stg_sessions) s

), sess as (

    select distinct

        s.distinct_session_id,
        s.session_id,
        s.user_id,
        s.ip,
        s.session_start_time as "TIME",
        lower(s.device_type) as device_type,
        lower(s.referrer) as referrer,
        lower(s.landing_page) as landing_page,
        lower(s.utm_source) as utm_source,
        lower(s.utm_campaign) as utm_campaign,
        lower(s.utm_medium) as utm_medium,
        lower(s.utm_term) as utm_term,
        lower(s.utm_primary_rollup) as utm_primary_rollup,
        lower(s.utm_primary_rollup_split) as utm_primary_rollup_split,
        lower(s.utm_digital_rollup) as utm_digital_rollup,
        lower(s.utm_platform) as utm_platform,
        lower(s.browser) as browser,
        lower(s.platform) as platform,
        s.business_entity,
        s.business_entity_language,
        s.first_session_date

    from stg_sessions s
--    left join {{var('web_analaytics_tool_schema')}}.all_pageviews pv on s.session_id = pv.session_id
--        and lower(domain) like '%website.ca%'
    {{dbt_utils.group_by(21)}}

), events as (

    SELECT
        session_id,
        event_table_name

    FROM stg_events
    WHERE session_id IN (SELECT session_id FROM sess)
    AND event_table_name IN (

        -- sa_view
        'lead_funnel_pageview__assessment',
        'lead_funnel_pageview__assessment_variants',
        'lead_funnel__assessment_v2',

        -- sa_complete
        'lead_funnel_pageview_completed__assessment',
        'lead_funnel_submission_sa',
        '_assessment_contentful_completed_sa',
        '_assessment_pl_s',
        '_assessment_pl_ns',

        -- lead_view
        'marketing_lander_pages_start_2a_get_started_',
        'marketing_lander_pages_start_1a_am_i_a_candidate_',
        'jebbit_landing_pages_discovery_sa_click_first_page_of_sa',

        -- lead_complete
        'marketing_lander_pages_start_thank_you_page',
        'jebbit_landing_pages_discovery_sa_submit_sa',

        --Teen flow
        'parent_teen_pageview_teens', --Pageview for /teens
        'parent_teen_submit_teen_lead_form',--complete 

        --Parents flow
        'parent_teen_pageview_parents',--Pageview for /parents
        'parent_teen_submit_parent_lead_form', --Complete the 
        'parent_teen_pageview_parent_video_confirmation',--after setting up the call on calendly the user will be taken here

        -- kit Funnel --
        'kit_1st_page_new_w_w_o_promo',
        'kit_shipping_2_',
        'view_2nd_kit_page_shipping_11_14_2016__organic_traffic_marketing_emails',
        'kit_payment_3_',
        'view_3rd_kit_page_payment_11_14_2016__organic_traffic_marketing_emails',
        'ik_funnel_pageview_ik_ty',

         -- Scan Funnel --
        'view_1st_scanning_page_new_','view_scanning_page_1st_page_',
        'successfully_scheduled_scan_appointment_1_2_',
        'completed_scan_appointment_booking_new_',
        'scan_order_funnel_shop_credit_card_page',
        'scans_view_shop_locations_page',
        'scans_view_shops_page',
        'shop_locations_page_view_unique_shop_page',
        'partner_network_view_appointment',
        'partner_network_appointment_confirmed',
        'contentful_scans_view_location_page', --new single location page 09/20/23
        'contentful_scans_view_scan_checkout_thank_you', --new Scan booking page 09/20/23
        'site_funnels_combo_any_initial_scan_funnel_page',
        'scan_funnel_combo_any_scan_funnel_booking',
        -- WEB CSP
        'smp_web_view_web_csp',
        -- sales Funnel
        'sales_order_flow_order_your_saless_button',
        'view_1st_sales_checkout_page_11_9_2016_',
        'view_2nd_sales_checkout_page_11_9_2016_',
        'view_3rd_sales_checkout_page_11_9_2016_',
        'confirmed_purchase_of_saless_page_11_9_2016_',

         -- Insurance Lead Funnel --
        'insurance_funnel_combo_insurance_get_started',
        'insurance_funnel_combo_policyholder_or_dependent',
        'insurance_funnel_submit_full_insurance_form',
        'insurance_funnel_view_insurance_page',
        'insurance_funnel_step1_find_benefits',
        'insurance_funnel_step2_accountholder',
        'insurance_funnel_step3_customer',
        'insurance_funnel_step4_policy',
        'insurance_funnel_step5_finish',

         -- Bypass 
        'bypass_funnel_click_get_started_banner', -- click on banner
        'bypass_funnel_submit_get_started_modal', -- submit zip on banner, not in use
        'bypass_funnel_pageview_product_lander_from_bypass','bypass_funnel_pageview_product_lander_bypass_no_zip', -- product lander from bypass banner
        'order_kit_or_scan_incl_cc_page_12_10_17', -- all kit and scan orders

        -- Shopify Funnel
        'shopify_view_shop_site','shopify_ca_view_shop_site', -- Visit Shopify page at shop.website.com or shop.website.ca
        'shopify_view_checkout_page', -- Any URL with /checkout/ and domain contains shop.directclub
        'shopify_checkout_view_customer_info_page','shopify_ca_checkout_view_customer_info_page', -- step=contact_information based on page title
        'shopify_checkout_view_shipping_page', -- step=shipping_method US CA
        'shopify_checkout_view_payment_page', -- step=payment_method CA US
        'shopify_checkout_view_thank_you_page','shopify_ca_checkout_view_thank_you_page', -- Title of "Thank you"
        'shopify_view_product_page', -- /products/ CA US
        'shopify_view_collection_page', -- /collections/ US CA
        'shopify_click_view_cart_count_wrapper', -- Click shopping cart number in cart CA US
        'shopify_click_view_cart_icon_wrapper', -- Click shopping cart icon  CA US
        'shopify_click_add_to_cart', -- Click "Add To Cart" button. Also shows cart CA US
        'shopify_checkout_view_order_page', --CA US view orders page

        -----------------------------------------------------------
        'reschedule_funnel_pageview_patient_portal', -- patient portal pageview

        --Retainer flow
        'cloudsite_retainer_page_view', --Pageview for /retainer
        'cloudsite_retainer_confirmation',--complete the retainer form

        -- care
        'view_any_care_page',
        'partner_network_appointment_confirmed'

        )

), current_customers_1 as (

  select distinct
  session_id,
  min(time) as first_event_time,
  1 as portal_event

  from stg_events where event_table_name = 'reschedule_funnel_pageview_patient_portal' -- user visits any page of the patient portal
  group by 1 order by 2 desc

), current_customers_2 as (

  select
  session_id,
  min(time) as first_event_time,
  1 as eval_order_event

  from stg_events where event_table_name in ('contentful_scans_view_scan_checkout_thank_you','ik_funnel_pageview_ik_ty',
  'successfully_scheduled_scan_appointment_1_2_','completed_scan_appointment_booking_new_','scan_order_funnel_shop_credit_card_page',
  'order_kit_or_scan_incl_cc_page_12_10_17') -- Eval events

  group by 1

), current_customers_3 as (

  select

      b.session_id,
      case when b.first_event_time > s.first_event_time then 0 else 1 end as portal_bf_sale -- did the user visited the portal before an eval?

  from current_customers_1 b
  left join current_customers_2 s on s.session_id = b.session_id

), d1 AS (

    SELECT

        s.distinct_session_id,
        s.session_id,
        s.user_id,
        s.ip,
        s."TIME"::date as "DATE",
        s.device_type as device,
        s.business_entity,
        s.business_entity_language,
        s.first_session_date,
        s.referrer,
        s.landing_page,
        s.utm_source,
        s.utm_campaign,
        s.utm_medium,
        s.utm_term,
        s.utm_primary_rollup,
        s.utm_primary_rollup_split,
        s.utm_digital_rollup,
        s.utm_platform,
        CASE -- simplifying Browser analysis 
            WHEN s.browser LIKE '%%firefox%%' THEN 'Firefox'
            WHEN s.browser LIKE '%%chrome%%' THEN 'Chrome'
            WHEN s.browser LIKE 'ie%%' THEN 'Internet Explorer'
            WHEN s.browser LIKE '%%internet explorer%%' THEN 'Internet Explorer'
            WHEN s.browser LIKE '%%safari%%' THEN 'Safari'
            WHEN s.browser LIKE '%%pinterest%%' THEN 'Pinterest App'
            WHEN s.browser LIKE '%%facebook%%' THEN 'Facebook App'
        ELSE 'Other' END AS browser,

    CASE    -- simplifying Source analysis 
            WHEN s.utm_source = 'snapchat' THEN 'Snapchat'
            WHEN s.utm_source = 'google' AND s.utm_campaign LIKE '%%competitors%%'           THEN 'Google Paid Search - Conquesting'
            WHEN s.utm_source = 'google' AND s.utm_campaign LIKE '%%branded%%'               THEN 'Google Paid Search - Branded'
            WHEN s.utm_source = 'google' AND s.utm_campaign NOT LIKE '%%branded%%'           THEN 'Google Paid Search - Non-Branded'
            WHEN s.utm_source = 'google' AND s.utm_campaign IS NULL AND s.utm_medium = 'cpc'   THEN 'Google Paid Search - Non-Branded'

            WHEN s.utm_source = 'bing' AND s.utm_campaign LIKE '%%competitors%%'             THEN 'Bing Paid Search - Conquesting'
            WHEN s.utm_source = 'bing' AND s.utm_campaign LIKE '%%branded%%'                 THEN 'Bing Paid Search - Branded'
            WHEN s.utm_source = 'bing' AND s.utm_campaign NOT LIKE '%%branded%%'             THEN 'Bing Paid Search - Non-Branded'
            WHEN s.utm_source = 'bing' AND s.utm_campaign IS NULL AND s.utm_medium = 'cpc'     THEN 'Bing Paid Search - Non-Branded'


            -- YAHOO
            when s.utm_source = 'yahoo' and s.utm_campaign like '%%competitors%%' then 'Yahoo Paid Search - Conquesting'
            when s.utm_source = 'yahoo' and s.utm_campaign like '%%branded%%' then 'Yahoo Paid Search - Branded'
            when s.utm_source = 'yahoo' and s.utm_medium = 'cpc' then 'Yahoo Paid Search - Non-Branded'
            when s.utm_source = 'yahoo' and (s.utm_medium = 'native' or s.utm_medium = 'nativeca') then 'Yahoo - Native'
            when s.utm_source = 'yahoo' and (s.utm_campaign like '%prsp%' or s.utm_medium like '%prsp%') then 'Yahoo - Prospecting'
            when s.utm_source = 'yahoo' and (s.utm_campaign like '%retargeting%' or s.utm_medium like '%rtgg%')then 'Yahoo - Retargeting'


            WHEN s.utm_source = 'pandora' THEN 'Pandora'

            WHEN s.utm_source = 'dataxu' AND s.utm_campaign LIKE '%%rtg%%' THEN 'DataXu - Retargeting'
            WHEN s.utm_source = 'dataxu' or s.utm_source = 'dataxuca'  or s.utm_source = 'dataxuau' THEN 'DataXu'
            WHEN s.utm_source = 'theskimm' THEN 'theSkimm'
            WHEN s.utm_source = 'centro' THEN 'Centro'

            WHEN s.utm_source IN ('trigger','adhoc','silverpopmailing','email') THEN 'E-mail'
            WHEN s.utm_medium = 'email' THEN 'E-mail'

            WHEN s.utm_source = 'groupon' THEN 'Groupon'
            WHEN s.utm_campaign LIKE '%%groupon%%' THEN 'Groupon'

            WHEN s.utm_source = 'taboola' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg')   THEN  'Taboola - Retargeting'
            WHEN s.utm_source = 'taboola' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')   THEN  'Taboola - Prospecting'
            WHEN s.utm_source = 'taboola'                                            THEN  'Taboola - Unknown'

            WHEN s.utm_source = 'outbrain' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg')  THEN  'Outbrain - Retargeting'
            WHEN s.utm_source = 'outbrain' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')  THEN  'Outbrain - Prospecting'
            WHEN s.utm_source = 'outbrain'                                            THEN  'Outbrain - Unknown'

            when s.utm_source = 'facebook'
                            and (s.utm_campaign like '%infl%'
                            or (s.utm_campaign in ('xxxx','YYY')
                                    and s.utm_term in ('XXX', 'YYY', ))--Actual campaing information is censored
                            or s.utm_campaign in ('******'))
                            THEN 'Facebook - Influencers'
                        WHEN s.utm_source = 'facebook' AND s.utm_campaign in ('6115796720030','geo-video-hf-an','6127401037230') THEN 'Facebook - Prospecting - Awareness'
                        WHEN s.utm_source = 'facebook' AND (s.utm_campaign in ('6115789297830','6109424936030')
                                                                                    OR s.utm_campaign = '6117747670830' and s.utm_term =    '6117747670230'
                                                                                    OR s.utm_campaign = '6115789297830' and s.utm_term =    '6115789298430')
                                THEN 'Facebook - Prospecting - Leads'
                        WHEN s.utm_source = 'facebook'
                            AND s.utm_campaign in ('******')
                            THEN 'Facebook - Prospecting - Sales'
                        WHEN s.utm_source = 'facebook'
                          AND s.utm_campaign in ('******')
                          THEN 'Facebook - Prospecting - Sales'
                        when s.utm_source = 'facebook'
                          and s.utm_campaign like '%campaign.id}%'
                          and s.utm_term like '%adset.id%'
                          and s.utm_medium = 'paidsocial'
                          then 'Facebook - Prospecting - Sales'
                        when s.utm_source = 'facebook'
                          and s.utm_campaign like '%campaign.id}%'
                          and s.utm_term like '%adset.id%'
                          and s.utm_medium = 'social'
                          then 'Facebook - Prospecting - Influencers'
                        WHEN s.utm_source = 'facebook' AND s.utm_campaign = '******'
                              or(s.utm_source = 'facebook' and s.utm_campaign in ( '******', '******') and s.utm_term in ('******','******'))
                          then 'Facebook - Retargeting'
            WHEN s.utm_source = 'facebook' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg' OR s.utm_campaign LIKE '%%retargeting%%')   THEN  'Facebook - Retargeting'
            WHEN s.utm_source = 'facebook' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')  THEN  'Facebook - Prospecting - Sales'
            WHEN s.utm_source = 'facebook' AND s.utm_medium = 'social' THEN 'Facebook - Social'
            WHEN s.utm_source = 'facebook'  THEN  'Facebook - Unknown'


            -- PINTEREST
            when s.utm_source = 'pinterest'
            	and s.utm_campaign in ('******', '******')
            	then 'Pinterest - Retargeting - Leads'
            when s.utm_source = 'pinterest'
            	and s.utm_campaign in ('******')
            	then 'Pinterest - Retargeting - Sales'
            when s.utm_source = 'pinterest'
                and (utm_def.ad_intention = 'retargeting'
                    or s.utm_medium = 'rtgg'
                    or s.utm_campaign like '%%retargeting%%'
                    or s.utm_campaign like '%rtg%'
                    or s.utm_medium like '%rtg%'
                    )  then  'Pinterest - Retargeting'

            when s.utm_source = 'pinterest'
                and (utm_def.ad_intention = 'prospecting'
                    or s.utm_medium like '%prsp%'
                    or s.utm_campaign like '%prsp%'
                    ) then  'Pinterest - Prospecting'

            when s.utm_source = 'pinterest' and s.utm_campaign in ('******', '******', '******', '******', '******') then 'Pinterest - Prospecting - Sales'

            when s.utm_source = 'pinterest' and s.utm_campaign in ('******','******')
              then 'Pinterest - Prospecting - Leads'

            when s.utm_source = 'pinterest' and s.utm_campaign in ('******','******' )
              then 'Pinterest - Prospecting - Awareness'

            when s.utm_source = 'pinterest' and s.utm_campaign in ('******','******')
              then 'Pinterest - Bonus Media - Awareness'

            when s.utm_source = 'pinterest' then 'Pinterest - Unknown'
            when s.utm_medium = 'paidsocialca' then 'Pinterest - Unknown'

            when s.utm_source = 'tinder' then 'Match Media Group - Tinder'

            when s.utm_source = 'yh' then 'Yellow Hammer'

            when s.utm_source = 'affiliates' then 'Affiliates'


            WHEN s.utm_source = 'instagram' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg' OR s.utm_campaign LIKE '%%retargeting%%')  THEN  'Instagram - Retargeting'
            WHEN s.utm_source = 'instagram' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')  THEN  'Instagram - Prospecting'
            WHEN s.utm_source = 'instagram'                                            THEN  'Instagram - Unknown'

            WHEN s.referrer LIKE '%%outbrain%%' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg')  THEN  'Outbrain - Retargeting'
            WHEN s.referrer LIKE '%%outbrain%%' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')  THEN  'Outbrain - Prospecting'
            WHEN s.referrer LIKE '%%outbrain%%'                                                                     THEN  'Outbrain - Unknown'

            WHEN s.referrer LIKE '%%taboola%%' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg')  THEN  'Taboola - Retargeting'
            WHEN s.referrer LIKE '%%taboola%%' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')  THEN  'Taboola - Prospecting'
            WHEN s.referrer LIKE '%%taboola%%'              THEN  'Taboola - Unknown'

            WHEN s.utm_source IS NULL and s.referrer LIKE '%%pandora%%' THEN 'Pandora'
            WHEN s.utm_source IS NULL and s.referrer LIKE '%%groupon%%' THEN 'Groupon'
            WHEN s.utm_source IS NULL and s.referrer LIKE '%%google%%' THEN 'Google Search - Organic'
            WHEN s.utm_source IS NULL and s.referrer LIKE '%%bing%%' THEN 'Bing Search - Organic'

            WHEN s.referrer LIKE '%%facebook%%'  AND  (utm_def.ad_intention = 'retargeting'  OR s.utm_medium = 'rtgg' OR s.utm_campaign LIKE '%%retargeting%%')   THEN  'Facebook - Retargeting'
            WHEN s.referrer LIKE '%%facebook%%'  AND  (utm_def.ad_intention = 'prospecting'  OR s.utm_medium = 'prsp')   THEN  'Facebook - Prospecting - Sales'
            WHEN s.referrer LIKE '%%facebook%%'                                                                        THEN  'Facebook - Unknown'

            WHEN s.referrer LIKE '%%pinterest%%' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg' OR s.utm_campaign LIKE '%%retargeting%%')    THEN  'Pinterest - Retargeting'
            WHEN s.referrer LIKE '%%pinterest%%' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')    THEN  'Pinterest - Prospecting'
            WHEN s.referrer LIKE '%%pinterest%%'                                                                       THEN  'Pinterest - Unknown'

            WHEN s.referrer LIKE '%%instagram%%' AND  (utm_def.ad_intention = 'retargeting' OR s.utm_medium = 'rtgg' OR s.utm_campaign LIKE '%%retargeting%%')    THEN  'Instagram - Retargeting'
            WHEN s.referrer LIKE '%%instagram%%' AND  (utm_def.ad_intention = 'prospecting' OR s.utm_medium = 'prsp')    THEN  'Instagram - Prospecting'
            WHEN s.referrer LIKE '%%instagram%%'                                                                       THEN  'Instagram - Unknown'

            WHEN s.referrer LIKE '%%utm_medium=email%%' THEN 'E-mail'
            WHEN s.referrer LIKE '%%3Demail%%' THEN 'E-mail'

            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer IS NULL AND s.landing_page = 'website.com/' THEN 'Direct Load - Homepage'
            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer = 'https://website.com/' AND s.landing_page = 'website.com/'
            THEN 'Direct Load - Homepage'

            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer IS NULL AND s.landing_page = 'website.ca/' THEN 'Direct Load - Homepage'
            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer = 'https://website.com/.ca/' AND s.landing_page = 'website.ca/'
            THEN 'Direct Load - Homepage'

            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer IS NULL AND s.landing_page LIKE 'website.com/%%' THEN 'Direct Load - Non-Homepage'
            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer LIKE '%%website.com%%' AND s.landing_page LIKE 'website.com/%%'
            THEN 'Prior SDC Visitor After Inactivity'

            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer IS NULL AND s.landing_page LIKE 'website.ca/%%' THEN 'Direct Load - Non-Homepage'
            WHEN s.utm_source IS NULL AND s.utm_campaign IS NULL AND s.referrer LIKE '%%website.ca%%' AND s.landing_page LIKE 'website.ca/%%'
            THEN 'Prior SDC Visitor After Inactivity'

        ELSE 'Other' END AS channel,


        CASE
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE 'brdcst_slead%' THEN 'Broadcast Emails Scan Leads'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE 'brdcst_klead%' THEN 'Broadcast Emails Kit Leads'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%brdcst%%' THEN 'Broadcast Emails with Auto'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%wbk%%' THEN 'Kit Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%wbs%%' THEN 'Scan Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%wb%%' THEN 'Kit/Scan Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%rb%%' THEN 'sales Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%gb%%' THEN 'Appointment Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%pb%%' THEN 'Kit Return Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%bb%%' THEN 'In-Treatment Stream'

            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%kslead%%' THEN 'Kit/Scan Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%klead%%' THEN 'Kit Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%slead%%' THEN 'Scan Lead Stream'

            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%kitst%%' THEN 'Kit Return Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%scanst%%' THEN 'Appointment Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%arlead%%' THEN 'sales Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%pauthst%%' THEN 'Pre-Auth Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%intreat%%' THEN 'In-Treatment Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%posttr%%' THEN 'Post-Treatment Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%insst%%' THEN 'Insurance Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%ialead%%' THEN 'Align Lead Stream'
            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%abcrt%%' THEN 'Abandon Cart Stream'

            WHEN s.utm_medium = 'email' AND s.utm_campaign LIKE '%%trig%%' THEN 'Transactional'

            WHEN s.referrer LIKE '%%utm_medium=email%%' THEN 'Other'
            WHEN s.referrer LIKE '%%3demail%%' THEN 'Other'
            WHEN s.utm_source IN ('trigger','adhoc','silverpopmailing') THEN 'Other'
            WHEN s.utm_medium = 'email' THEN 'Other'
        ELSE NULL END AS email_stream,

        CASE
            WHEN s.platform LIKE '%%android%%' THEN 'Android'
            WHEN s.platform LIKE '%%windows%%' THEN 'Windows'
            WHEN s.platform LIKE '%%chrome os%%' THEN 'Chrome OS'
            WHEN s.platform LIKE '%%ios%%' THEN 'iOS'
            WHEN s.platform LIKE '%%mac os%%' THEN 'Mac OS'
        ELSE 'Other' END AS platform,

       --------------------Analysis by wheter or not the event happened during the session --------------------

        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('smp_web_view_web_csp') THEN s.user_id END) as view_web_csp,

        --  Assessment Funnel --
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('lead_funnel_pageview__assessment', 'lead_funnel_pageview__assessment_variants',
        'lead_funnel__assessment_v2') or e.event_table_name = 'snapchat_landing_pages_total_snapchat_leads' THEN s.user_id END) as sa_view,

        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('lead_funnel_pageview_completed__assessment','lead_funnel_submission_sa',
        '_assessment_contentful_completed_sa','_assessment_pl_s','_assessment_pl_ns') THEN s.user_id END) as sa_complete, --product lander

        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('marketing_lander_pages_start_2a_get_started_',
        'marketing_lander_pages_start_1a_am_i_a_candidate_','jebbit_landing_pages_discovery_sa_click_first_page_of_sa') THEN s.user_id END) as lead_view,

        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('marketing_lander_pages_start_thank_you_page',
        'jebbit_landing_pages_discovery_sa_submit_sa') THEN s.user_id END) as lead_complete,

        -- Assessment Teen Funnel --
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('parent_teen_pageview_teens') THEN s.user_id END) as sa_teen_view,
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('parent_teen_submit_teen_lead_form') THEN s.user_id END) as sa_teen_complete,
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ( 'parent_teen_pageview_parents') THEN s.user_id END) as sa_teen_parent_view,
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('parent_teen_submit_parent_lead_form') THEN s.user_id END) as sa_teen_parent_complete,

        --Parents booking a call funnel --
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('parent_teen_click_video_consult_cta') THEN s.user_id END) as PARENT_VIDEO_CONSULT_CTA,
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('parent_teen_pageview_parent_video_confirmation') THEN s.user_id END) as parent_video_consult_complete,

        --Retainers--
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('cloudsite_retainer_page_view') THEN s.user_id END) as retainer_page_view,
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('cloudsite_retainer_confirmation') THEN s.user_id END) as retainer_confirmation,

        -- kit Funnel --
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('kit_1st_page_new_w_w_o_promo') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS  ik_product_page,
CASE WHEN SUM(CASE WHEN e.event_table_name IN ('view_2nd_kit_page_shipping_11_14_2016__organic_traffic_marketing_emails') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS ik_shipping_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('kit_payment_3_','view_3rd_kit_page_payment_11_14_2016__organic_traffic_marketing_emails') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS ik_payment_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('ik_funnel_pageview_ik_ty') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS ik_thanks_page,
/***************************************************************************************************************************************************/
         -- Appointment Funnel --
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('site_funnels_combo_any_initial_scan_funnel_page') THEN 1 END) --- /any initial scan funnel page
            > 0 THEN 1 ELSE 0 END AS any_scan_start_page, --
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('scan_funnel_combo_any_scan_funnel_booking') THEN 1 END) --- /any initial scan funnel page
            > 0 THEN 1 ELSE 0 END AS any_scan_complete_page, --

         
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('view_1st_scanning_page_new_','view_scanning_page_1st_page_','partner_network_view_appointment') THEN 1 END) --- /checkout and query: w=scekss
            > 0 THEN 1 ELSE 0 END AS scan_book_page, -- 
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('successfully_scheduled_scan_appointment_1_2_','completed_scan_appointment_booking_new_'
            ,'scan_order_funnel_shop_credit_card_page','partner_network_appointment_confirmed','contentful_scans_view_scan_checkout_thank_you') THEN 1 END) -- Any completion scan completion.
            > 0 THEN 1 ELSE 0 END AS scan_complete_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('scans_view_shop_locations_page', 'scans_view_shops_page',
        'shop_locations_page_view_unique_shop_page') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS scan_locations_book_page, -- the user visited the shops page or individual locations page during the session      
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('contentful_scans_view_location_page') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS scan_single_location_page, -- Contentful scan location page after 9/18/23 this one replaced the the traditional single locations page 

/***************************************************************************************************************************************************/

         -- sales Purchase Checkout / non commerce tools flow --

        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('view_1st_sales_checkout_page_11_9_2016_') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS sales_checkout_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('view_2nd_sales_checkout_page_11_9_2016_') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS sales_shipping_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('view_3rd_sales_checkout_page_11_9_2016_') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS sales_payment_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('confirmed_purchase_of_saless_page_11_9_2016_') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS sales_purchase_page,


         -- Insurance Lead Funnel --

        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_combo_insurance_get_started','insurance_funnel_view_insurance_page') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS rcm_lead_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_combo_policyholder_or_dependent','insurance_funnel_step1_find_benefits') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS rcm_submit_info,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_submit_full_insurance_form','insurance_funnel_step5_finish') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS rcm_submit_form,

            -- Insurance Lead Funnel 2021 --
            CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_view_insurance_page') THEN 1 END)
                > 0 THEN 1 ELSE 0 END AS rcm_view_insurance_page,
            CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_step1_find_benefits') THEN 1 END)
                > 0 THEN 1 ELSE 0 END AS rcm_benefits,
            CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_step2_accountholder') THEN 1 END)
                > 0 THEN 1 ELSE 0 END AS rcm_accountholder,
            CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_step3_customer') THEN 1 END)
                > 0 THEN 1 ELSE 0 END AS rcm_customer,
            CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_step4_policy') THEN 1 END)
                > 0 THEN 1 ELSE 0 END AS rcm_policy,
            CASE WHEN SUM(CASE WHEN e.event_table_name IN ('insurance_funnel_step5_finish') THEN 1 END)
                > 0 THEN 1 ELSE 0 END AS rcm_finish,


         -- Bypass SA
         CASE WHEN SUM(CASE WHEN e.event_table_name IN ('bypass_funnel_click_get_started_banner') THEN 1 END)
             > 0 THEN 1 ELSE 0 END AS bypass_banner_click,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('bypass_funnel_submit_get_started_modal') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS bypass_sa_submit_info,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('bypass_funnel_pageview_product_lander_from_bypass','bypass_funnel_pageview_product_lander_bypass_no_zip') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS bypass_sa_prod_page,
        CASE WHEN SUM(CASE WHEN e.event_table_name IN ('order_kit_or_scan_incl_cc_page_12_10_17') THEN 1 END)
            > 0 THEN 1 ELSE 0 END AS bypass_sa_order_page,

        -- Shopify Funnel
        case when sum(case when e.event_table_name in ('shopify_view_shop_site','shopify_ca_view_shop_site') then 1 end)
            > 0 then 1 else 0 end as shopify_visit_shop,

        case when sum(case when e.event_table_name in ('shopify_view_collection_page') then 1 end)
            > 0 then 1 else 0 end as shopify_view_collection,

        case when sum(case when e.event_table_name in ('shopify_view_product_page') then 1 end)
            > 0 then 1 else 0 end as shopify_view_product,

        case when sum(case when e.event_table_name in ('shopify_click_view_cart_count_wrapper', 'shopify_click_view_cart_icon_wrapper') then 1 end)
            > 0 then 1 else 0 end as shopify_view_cart,

        case when sum(case when e.event_table_name in ('shopify_click_add_to_cart') then 1 end)
            > 0 then 1 else 0 end as shopify_add_to_cart,

        case when sum(case when e.event_table_name in ('shopify_view_checkout_page') then 1 end)
            > 0 then 1 else 0 end as shopify_view_checkout,

        case when sum(case when e.event_table_name in ('shopify_checkout_view_customer_info_page','shopify_ca_checkout_view_customer_info_page') then 1 end)
            > 0 then 1 else 0 end as shopify_checkout_customer_info,

        case when sum(case when e.event_table_name in ('shopify_checkout_view_shipping_page') then 1 end)
            > 0 then 1 else 0 end as shopify_checkout_shipping,

        case when sum(case when e.event_table_name in ('shopify_checkout_view_payment_page') then 1 end)
            > 0 then 1 else 0 end as shopify_checkout_payment,

        case when sum(case when e.event_table_name in ('shopify_checkout_view_thank_you_page','shopify_ca_checkout_view_thank_you_page') then 1 end)
            > 0 then 1 else 0 end as shopify_checkout_thanks,

    --Care+
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('view_any_care_page') THEN s.user_id END) as view_any_care_plus_page,
        COUNT(DISTINCT CASE WHEN e.event_table_name IN ('partner_network_appointment_confirmed') THEN s.user_id END) as pn_appt_confirmed,
  
    ----
        COUNT(DISTINCT CASE WHEN cc.portal_bf_sale = 1 THEN s.user_id END) as patient_portal


        from sess s
        left join events e on s.session_id = e.session_id
        left join (select * from utm_def) as utm_def on s.utm_campaign = utm_def.ad_campaign
        left join current_customers_3 cc on cc.session_id = s.session_id

        {{dbt_utils.group_by(23)}}

), d2 AS (

    -- Add in New v. Returning User info. Remove page counts where user skips a step in funnel
    select distinct

        d1."DATE",
        d1.user_id,
        d1.distinct_session_id,
        d1.session_id,
        d1.device,
        d1.channel,
        d1.utm_primary_rollup,
        d1.utm_primary_rollup_split,
        d1.utm_medium,
        d1.utm_source,
        d1.utm_platform,
        d1.utm_digital_rollup,
        d1.browser,
        d1.email_stream,
        d1.platform,
        d1.business_entity,
        d1.business_entity_language,
        d1.first_session_date::date as first_session_date,
        view_web_csp,
        -- Lead &  Assessment Funnel --
        CASE WHEN sa_view = 1 OR lead_view = 1 THEN 1 ELSE 0 END AS lead_view,
        CASE WHEN (sa_view = 1 AND sa_complete = 1) OR (lead_view = 1 AND lead_complete = 1) THEN 1 ELSE 0 END AS lead_complete,

        -- Teen SA Funnel --
        CASE WHEN sa_teen_view = 1 THEN 1 ELSE 0 END AS teen_sa_view,
        CASE WHEN (sa_teen_view = 1 AND sa_teen_complete = 1) THEN 1 ELSE 0 END AS teen_sa_complete,

        -- Teen SA Funnel for Parents --
        CASE WHEN sa_teen_parent_view = 1 THEN 1 ELSE 0 END AS teen_sa_parent_view,
        CASE WHEN (sa_teen_parent_view = 1 AND sa_teen_parent_complete = 1) THEN 1 ELSE 0 END AS teen_sa_parent_complete,

        -- Parents booking a call funnel--
        CASE WHEN PARENT_VIDEO_CONSULT_CTA = 1 THEN 1 ELSE 0 END AS PARENT_VIDEO_CONSULT_CTA,
        CASE WHEN (PARENT_VIDEO_CONSULT_CTA = 1 AND parent_video_consult_complete = 1) THEN 1 ELSE 0 END AS parent_video_consult_complete,

        --  Assessment Funnel --
        sa_view,
        CASE WHEN sa_view = 0 THEN 0 else sa_complete END as sa_complete,

        -- care --
        view_any_care_plus_page,
        pn_appt_confirmed,
        CASE WHEN view_any_care_plus_page = 0 THEN 0 else pn_appt_confirmed END as pn_appt_confirmed_for_care,

        -- Retainer  --
        retainer_page_view,
        CASE WHEN retainer_page_view = 0 THEN 0 else retainer_confirmation END as retainer_confirmation,

        -- kit Funnel --
        case when scan_book_page = 1 then 0 else ik_product_page end as ik_product_page,
        ik_shipping_page,
        ik_payment_page,
        /*CASE WHEN ik_product_page = 0 THEN 0 ELSE ik_shipping_page END AS ik_shipping_page,
        CASE WHEN ik_shipping_page = 0 THEN 0 ELSE ik_payment_page END as ik_payment_page,*/
        CASE WHEN ik_product_page = 0 THEN 0 ELSE ik_thanks_page END as ik_thanks_page_temp,

        -- Scan Funnel --
        -- The Locations funnel and the reg. Scan funnel both share the same events for completing a Appointment.
        -- As such, need to define here that if they went through the regular funnel and ordered, only count them there
        -- and not here, or else we would be double coutning them

        scan_book_page, -- The user visited checkout
        scan_locations_book_page, --The user visited locations
        scan_single_location_page,-- the user visited a single location
        any_scan_start_page,
        any_scan_complete_page,
        CASE WHEN scan_book_page =0 THEN 0 ELSE scan_complete_page END as scan_complete_page_temp,
        CASE WHEN scan_locations_book_page =0 then 0 ELSE scan_complete_page END as scan_locations_complete_page_temp,
        CASE WHEN scan_single_location_page =0 then 0 ELSE scan_complete_page END as scan_single_location_complete_page_temp,

    
        -- sales Purchase Funnel --
        sales_checkout_page,
       CASE WHEN sales_checkout_page = 0 THEN 0 ELSE sales_shipping_page END as sales_shipping_page,
       CASE WHEN sales_shipping_page = 0 THEN 0 ELSE sales_payment_page END as sales_payment_page,
       /*CASE WHEN sales_payment_page = 0 THEN 0 ELSE sales_purchase_page END as*/ sales_purchase_page,

        -- Insurance Lead Funnel --
        rcm_lead_page,
        CASE WHEN rcm_lead_page = 0 THEN 0 ELSE rcm_submit_info END AS rcm_submit_info,
        CASE WHEN rcm_submit_info = 0 THEN 0 ELSE rcm_submit_form END AS rcm_submit_form,
        rcm_view_insurance_page,
        rcm_benefits,
        rcm_accountholder,
        rcm_customer,
        rcm_policy,
        rcm_finish,

        -- Bypass SA Funnel
        bypass_banner_click,
        bypass_sa_prod_page,
        CASE WHEN bypass_sa_prod_page  = 0 THEN 0 ELSE  bypass_sa_order_page  END AS bypass_sa_order_page_temp,
        case when bypass_sa_prod_page = 0 then 0 else scan_complete_page end as bypass_scan_complete_page_temp,
        case when bypass_sa_prod_page = 0 then 0 else ik_thanks_page end as bypass_kit_thanks_page,

        -- Shopify Funnel
        shopify_visit_shop,
        shopify_view_collection,
        shopify_view_product,
        shopify_view_cart,
        shopify_add_to_cart,

        --shopify_view_checkout,
        shopify_checkout_customer_info,
        shopify_checkout_shipping,
        shopify_checkout_payment,
        shopify_checkout_thanks,

        patient_portal


        FROM d1

)--, d3 as (

    select

        d2.*,


        --  Booking from checkout exclusively (Since 17/10/23 no longer an option for US)
        case
            when scan_complete_page_temp = 1 
            and scan_single_location_complete_page_temp= 0 
            and scan_locations_complete_page_temp = 0 
            then 1 
        else 0 end as scan_complete_page,

        --  Booking from shops exclusively  (Since 17/10/23 no longer an option for US)
        case
            when scan_complete_page_temp = 0 
            and scan_single_location_complete_page_temp = 0 
            and scan_locations_complete_page_temp = 1 
            then 1 
        else 0 end as scan_locations_complete_page,

        --  Booking from single location exclusively (Only available after 17/10/23 for US)
        case
            when scan_complete_page_temp = 0
            and scan_single_location_complete_page_temp = 1 
            and scan_locations_complete_page_temp = 0 
            then 1
        else 0 end as single_scan_location_complete_page,
       /**************************************************************************************************************************************/

        --  Booking from single location after visiting Scan checkout and without visiting /shops
        case
        when scan_single_location_complete_page_temp =1 
        and scan_complete_page_temp =1 
        and scan_locations_complete_page_temp =0 
        then 1 -- 
        else 0 end as scan_location_complete_after_checkout_page,

        --  Booking from single location after visiting /shops and without visiting Scan checkout
        case
        when scan_single_location_complete_page_temp =1 
        and scan_locations_complete_page_temp =1
        and scan_complete_page_temp =0 
        then 1 -- 
        else 0 end as scan_location_complete_after_shops_page,
   
        --  Booking from single location after viewing the web CSP
        case
        when any_scan_complete_page =1 
        and view_web_csp =1
        then 1 -- 
        else 0 end as scan_location_complete_after_web_csp,

        --- if a user books a scan from both bypass funnel and locations funnel in the same session, locations gets the booking
        case
            when scan_locations_complete_page_temp = 0 
            and bypass_scan_complete_page_temp = 1 then 1
        else 0 end as bypass_sa_order_page,

        --- if a user books a scan from both bypass funnel and scan funnel but not locations funnel in the same session, bypass gets the booking
        case
            when bypass_scan_complete_page_temp = 0
            and scan_complete_page_temp = 1 then 1

        --- if a user books a kit from bypass, count as bypass kit, else count as regular ik kit
        
            when scan_complete_page =1 or scan_locations_complete_page =1 then 1 else 0 end as non_by_pass_scan_completion,
        case
            when bypass_sa_order_page_temp = 0
            and ik_thanks_page_temp = 1 then 1
        else 0 end as ik_thanks_page

    from d2
