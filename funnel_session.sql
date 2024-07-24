{{
    config(
        materialized = 'table',
        unique_key = 'distinct_session_id'
    )
}}
--Notes: this model captures at a session level the time of the most important events on the APP funnel and attempts to link those events to a web user.
-- Created by Jesus Montero

with sessions as ( --getting raw session data for app only sessions
    select 
        distinct
        user_id,
        session_id,
        {{ dbt_utils.surrogate_key(['session_id', 'user_id', 'session_start_time'])}} as distinct_session_id,
        time,
        session_start_time,
        library,
        platform,
        device_type,
        country,
        region,
        city,
        ip
    from {{ref('web_analytics_tool_sessions')}} hs
    left join {{ref('web_analytics_tool_events')}}
    using(user_id,session_id)
    where event_table_name in ('general_app_ios_view_welcome_screen','general_app_ios_view_waiting_room',
    'general_app_ios_view_scan_live_lobby','general_app_ios_view_onboarding','general_app_ios_view_custom_smile_plan_viewer', 
    'first_start_smile_maker_video','general_app_ios_nsdk_view_contact_information','general_app_ios_nsdk_view_learn_more',
    'general_app_ios_nsdk_view_welcome_screen','general_app_ios_nsdk_view_waiting_room','general_app_ios_nsdk_view_welcome_screen',
    'general_app_ios_nsdk_view_phonecapture','general_app_ios_nsdk_view_scan_live_lobby', 'general_app_android_view_email_capture',
    'general_app_android_view_phone_capture','general_app_android_view_welcome_screen','general_app_android_view_csp','custom_events_general_app_android_back_alert_end_pressed'
    ) --Using all the possible view events
),
simplified_app_to_web as (
select 
distinct
PRIMARY_ACCOUNT_ID,
ACCOUNT_ID_web_analytics_tool_USER,
ACS_web_analytics_tool_USER_ID,
ACCOUNT_ID_web_analytics_tool_USER_FIRST_SESSION
from {{ref('app_to_web')}}
where account_id_web_analytics_tool_user is not null 
),
account_service as (
     /*switching to use new account service web_analytics_tool id table that rolls up to primary_account_id*/
    select distinct
	
	  reference_number,
	  primary_account_id
	  
    from account_service_web_analytics_tool_id --removing ref tag for dbt run efficiency
),
events as ( --getting only general_app events
    select * from {{ref('web_analytics_tool_events')}} h
    left join simplified_app_to_web a
    on h.user_id::varchar = a.ACS_web_analytics_tool_USER_ID::varchar and h.time<a.account_id_web_analytics_tool_user_first_session
    where event_table_name like 'general_app_ios_%' or event_table_name in ('enteredlivelobby') or event_table_name like '%general_app_android_%'
), 
events_agg as (--Getting the session end time and the total number of events per user
  select
  distinct
        user_id,
        session_id,
        max(time) as session_end_time,
        count(*) as event_count,
        sum(case when event_table_name in ('general_app_ios_tap_share_3d_model','general_app_ios_nsdk_tap_share_csp_3d_viewer') then 1 else 0 end) as CSP_shared_count
  from events
  group by 1, 2
), 
min_sess as (--Getting the very first time a user visited us
        select
        distinct
        user_id,
        min(session_start_time) as first_session_date
    from sessions
    group by 1
),
sessions_joined as (--We join all the session level data in this CTE and create the main key of the table.
    select
        s.*,
        a.primary_account_id,
        ms.first_session_date,
        ea.session_end_time,
        ea.event_count,
        ea.CSP_shared_count
    from sessions s
    left join account_service a on s.user_id::varchar = a.reference_number
    left join events_agg ea on s.session_id = ea.session_id and s.user_id = ea.user_id
    left join min_sess ms on s.user_id = ms.user_id
), 
session_events as ( -- This CTE reduces the information from the events table to only session and event table 
    select
        distinct
        session_id,
        time as event_time,
        event_table_name,
        ACCOUNT_ID_web_analytics_tool_USER_FIRST_SESSION
    from events
), 
windowsf as ( --To have one row per session I use windown functions to determine the first or last time an event happened, since it can happend multiple times during a session
select
    distinct session_id,
    first_value((case when event_table_name  in ('general_app_ios_tap_initiate_smile_scanner','general_app_ios_tap_start_your_scan_on_sf','enteredlivelobby','general_app_android_start_your_scan') then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_3ds_start_time, -- First time when the user started the 3D scan

        first_value((case when event_table_name in ('custom_events_general_app_android_back_alert_end_pressed')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_android_back_alert_end, -- Back Alert

        first_value((case when event_table_name in ('general_app_ios_view_contact_information','general_app_ios_nsdk_view_contact_information','general_app_android_view_email_capture')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_3ds_end_time_temp, -- First time when the user completed the 3D scan
        
        case when first_3ds_end_time_temp > first_3ds_start_time and first_android_back_alert_end is null then first_3ds_end_time_temp else null end as first_3ds_end_time,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_phonecapture','general_app_ios_view_phone_capture', 'general_app_android_view_phone_capture','general_app_android_view_phone_capture')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_view_phone_capture_end_time, -- First time when the user sees the phone number option

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_no_thanks_phone_opt_out','general_app_ios_tap_no_thanks_opt_in')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_opt_out_time, -- User opt out if this is null but phone capture is true the user opted in

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_waiting_room','general_app_ios_view_waiting_room', 'general_app_android_waiting_room')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_entered_waitroom_time, -- First time when the user completed the 3D scan and provides their email

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_sms_tap_opt_in','general_app_ios_tap_opt_in')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as text_notification_opt_in, -- SMS opt in 

       first_value((case when event_table_name in ('general_app_ios_tap_email_provided','general_app_ios_nsdk_tap_email_provided','general_app_android_view_phone_capture')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as email_notification_enable,-- email provided 

         first_value((case when event_table_name = 'general_app_ios_nsdk_alert_csp_ready'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as viewed_CSP_ready_popup,-- Custom Smile Plan Ready Pop Up

        first_value((case when event_table_name = 'general_app_ios_view_error_message'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as CSP_failed, -- Default error screen viewed

        first_value
        (
            (
                case 
                when event_table_name in ('general_app_ios_nsdk_view_scan_live_lobby','general_app_ios_view_custom_smile_plan_viewer', 'general_app_android_view_csp') 
                or ( event_table_name in ('general_app_ios_view_contact_information','general_app_ios_nsdk_view_contact_information','general_app_android_view_email_capture') 
                and ACCOUNT_ID_web_analytics_tool_USER_FIRST_SESSION is not null) 
                then coalesce(event_time,ACCOUNT_ID_web_analytics_tool_USER_FIRST_SESSION) else null end
            ) ignore nulls
        )   
        over 
        (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_viewed_CSP_time,  -- Users sees the CSP 

        last_value
        (
            (
                case when event_table_name in ('general_app_ios_nsdk_view_scan_live_lobby','general_app_ios_view_custom_smile_plan_viewer', 'general_app_android_view_csp') 
                or (event_table_name in ('general_app_ios_view_contact_information','general_app_ios_nsdk_view_contact_information','general_app_android_view_email_capture') 
                and ACCOUNT_ID_web_analytics_tool_USER_FIRST_SESSION is not null)  
                then coalesce(event_time,ACCOUNT_ID_web_analytics_tool_USER_FIRST_SESSION) else null end
            ) ignore nulls
        ) 
        over 
        (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as last_viewed_CSP_time,  -- Last time during the session the customer views the CSP 

        first_value((case when event_table_name in ('general_app_ios_combo_add_to_cart_on_3d_viewer','general_app_ios_tap_share_3d_model')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as first_CSP_shared_time, -- Time when the model was shared first 

        last_value(event_table_name) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as last_event_name,-- Last event of the session

        first_value((case when event_table_name in  ('general_app_ios_nsdk_see_my_options_combo','general_app_ios_combo_add_to_cart_on_3d_viewer')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following
		) as clicked_on_buy_now_time, -- user clicks on buy now 

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_scan_live_lobby','general_app_ios_view_custom_smile_plan_viewer', 'general_app_android_view_csp') then event_time else null end) ignore nulls) over (
                partition by session_id
                order by event_time
                rows between unbounded preceding and unbounded following
            ) as first_CSP_view_event, -- time of the first time a user sees the CSP 

        last_value((case when event_table_name in ('general_app_ios_nsdk_tap_interaction_with_3d_viewer','general_app_ios_tap_interaction_with_3d_viewer')  then event_time else null end) ignore nulls) over (
                partition by session_id
                order by event_time
                rows between unbounded preceding and unbounded following
            ) as last_CSP_interaction_event, -- The time of the last interaction with the 3D viewer 
        

        datediff(second,first_CSP_view_event, last_CSP_interaction_event)/60 as CSP_interaction_time_minutes, -- time between the first csp view and the last interaction

        first_value((case when event_table_name in ('general_app_ios_view_welcome_screen','general_app_ios_nsdk_view_welcome_screen', 'general_app_android_view_welcome_screen')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_welcome_screen_view, --- time of first time a user enters the welcome screen

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_enable_notifications_pop_up','general_app_ios_tap_allow_notifications')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_allow_notifications_click, --- time of first time user taps allow notification

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_sign_in_welcome_screen','general_app_ios_tap_sign_in_welcome_screen')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_tap_sign_in_welcome_screen, -- time of first time a user taps sign in welcome screen 
       
        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_launch_smile_maker_welcome_screen','general_app_ios_tap_launch_smile_maker','general_app_android_start_your_scan')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_Launch_general_app_Smile_Maker, -- time of first time user launches the general_app smile maker
     
        first_value((case when event_table_name = 'general_app_ios_tap_buy_now_welcome_screen'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_tap_buy_now_welcome_screen, --time of first time user taps buy now welcome screen

        first_value((case when event_table_name = 'general_app_ios_nsdk_tap_learn_more_welcome_screen'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_tap_learn_more, --first time user views learn more

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_go_back_waiting_room','general_app_ios_tap_go_back_waiting_room')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_tap_go_back_waiting_room, --time when user decides to leave waiting room and start process over
     
        first_value((case when event_table_name in ('general_app_ios_view_smile_maker_video','general_app_android_start_video')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_start_smile_maker_video, -- time when user first enters the smile maker video

        first_value((case when event_table_name = 'general_app_ios_tap_go_back_smile_maker_video'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_tap_go_back_smile_maker_video, -- time when user first decides to go back from skipping the video
                  
        first_value((case when event_table_name = 'general_app_ios_tap_already_watched_smile_maker_video'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_tap_already_watched_smile_maker_video, --time when user first skips video in first loop and continues the skip
      
        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_skip','general_app_ios_tap_skip_smile_maker_video','general_app_android_tap_skip_video')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_skip_smile_maker_video, -- time when user first skips the smile maker video
          
        first_value((case when event_table_name = 'general_app_ios_view_onboarding'  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_start_user_onboarding, -- time when user first enters the onboarding screen
            
        datediff(second, first_start_smile_maker_video, first_start_user_onboarding)/60 as smile_maker_video_time, --time between smile maker video and user onboarding
                  
        datediff(second, first_start_smile_maker_video, ifnull(first_tap_already_watched_smile_maker_video, first_skip_smile_maker_video))/60 as skip_smile_maker_video_time, --time between smile maker video and skip smile maker

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_scan_live_lobby','general_app_ios_view_scan_live_lobby','general_app_android_view_csp')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following) as first_scan_live_lobby_view, -- first time user enters live lobby view

        first_value((case when event_table_name in ('general_app_ios_tap_get_scanning', 'general_app_android_start_your_scan', 'general_app_android_start_your_scan')  then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_click_get_scanning, -- time when user first clicks scanning

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_enable_camera_access','general_app_ios_tap_enable_camera_access', 'general_app_android_enable_camera')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_enable_camera_access_click, --time users clicks enable camera access

        ------- Questionnaire ----------------------------------------------------------------

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_start_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_start_questionnaire,

        first_value((case when event_table_name in ('general_app_ios_nsdk_tap_not_right_now_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_click_questionnaire_not_now,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_questionnaire_information')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_questionnaire_information,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_questionnaire_stackla')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_questionnaire_stackla,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_testimony_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_testimony_questionnaire,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_aligner_options_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_aligner_options_questionnaire,
        
        first_value((case when event_table_name in ('general_app_ios_nsdk_view_pricing_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_pricing_questionnaire,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_lsg_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_lsg_questionnaire,

        first_value((case when event_table_name in ('general_app_ios_nsdk_view_finish_questionnaire')   then event_time else null end) ignore nulls) over (
			partition by session_id
			order by event_time
			rows between unbounded preceding and unbounded following)
            as first_view_finish_questionnaire


        
from 
session_events

),

combined as ( --final CTE

    select
    * -- All the web_analytics_tool sessions data
    from sessions_joined sj
    left join windowsf using(session_id) 

)

select * 
from combined