import streamlit as st
import awswrangler as wr
import datetime
import pandas as pd
import yaml
import hashlib
import time

# Initialize session state for authentication
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'login_time' not in st.session_state:
    st.session_state.login_time = None

def load_config():
    with open('config.yaml') as file:
        return yaml.safe_load(file)

def hash_password(password):
    """Hash a password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def check_session_timeout():
    """Check if the session has timed out (30 minutes)"""
    if st.session_state.login_time is None:
        return True
    return (time.time() - st.session_state.login_time) > 1800  # 30 minutes timeout

def login_page():
    st.title("Login")
    config = load_config()
    
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")
        
        if submit:
            if username in config['credentials']['usernames']:
                stored_password = config['credentials']['usernames'][username]['password']
                if password == stored_password:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.login_time = time.time()
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid password")
            else:
                st.error("Invalid username")

def main_dashboard():
    st.set_page_config(page_title="Deploy to Ride Count Dashboard", layout='wide')
    st.title("Deploy to Ride Count Dashboard")
    
    # Add logout button in sidebar
    if st.sidebar.button("Logout"):
        st.session_state.authenticated = False
        st.session_state.username = None
        st.session_state.login_time = None
        st.rerun()
    
    # Display username and session info
    st.sidebar.write(f"Logged in as: {st.session_state.username}")
    if st.session_state.login_time:
        time_left = 1800 - (time.time() - st.session_state.login_time)
        if time_left > 0:
            st.sidebar.write(f"Session expires in: {int(time_left/60)} minutes")
        else:
            st.session_state.authenticated = False
            st.rerun()

    # Sidebar filters
    start_date = st.sidebar.date_input("Start Date", value=datetime.date(2025, 5, 1))
    end_date = st.sidebar.date_input("End Date", value=datetime.date(2025, 5, 10))

    if start_date > end_date:
        st.sidebar.error("Start Date must be before End Date")
    else:
        if st.sidebar.button("Run Query"):
            with st.spinner("Running query..."):
                sql = f"""
-- Filtre Tarihleri '{start_date}' ile '{end_date}'
USING
EXTERNAL FUNCTION st_pointfromgeohash(geohash VARCHAR) RETURNS VARCHAR LAMBDA 'athena-udf-handler',
EXTERNAL FUNCTION h3_to_geo(h3_address VARCHAR) RETURNS VARCHAR LAMBDA 'h3-athena-udf-handler',
EXTERNAL FUNCTION geo_to_h3_address(lat DOUBLE, lng DOUBLE, res INTEGER) RETURNS VARCHAR LAMBDA 'h3-athena-udf-handler'

WITH rebelance_collect_tasks as (
    select id as c_task_id,
           scooter_id as c_scooter_id,
           cast(created_date as timestamp) as c_created_date,
           cast(start_date as timestamp) as c_start_date,
           cast(end_date as timestamp) as c_end_date,
           date_add('hour', 12, start_date) as expected_rebalance
    from martimain.temporary_tasks
    where snapshot_date = current_date - interval '1' day
          and start_date >= cast('2024-01-01' as date)
          and task_type = 2
          and status = 5
),

rebelance_deploy_tasks as (
    select id as dp_task_id,
           scooter_id as dp_scooter_id,
           cast(created_date as timestamp) as dp_created_date,
           cast(start_date as timestamp) as dp_start_date,
           cast(end_date as timestamp) as dp_end_date,
           task_location as deployed_location,
           date_add('hour', 24, end_date) as for_ride_in24h
    from martimain.temporary_tasks
    where snapshot_date = current_date - interval '1' day
          and start_date >= cast('2024-01-01' as date)
          and task_type = 16
          and status = 5
),

rebalance_tasks as (
    select rbc.*, rdp.*, 'rebalance' as deploy_type
    from rebelance_collect_tasks rbc
    left join rebelance_deploy_tasks rdp on (rbc.c_scooter_id = rdp.dp_scooter_id)
    where c_end_date <= dp_start_date
      and dp_end_date <= expected_rebalance
),

finding_duplicate_rebalance as (
    select *, row_number() over (partition by dp_task_id order by c_end_date desc) as recent_rebalance
    from rebalance_tasks
),

no_duplicate_rebalance_tasks as (
    select *
    from finding_duplicate_rebalance
    where recent_rebalance = 1
),

deploy_tasks as (
    select 
           t.id as task_id,
           t.scooter_id,
           sbv.name as scooter_body_version_name,
           t.geofence_group,
           to_location,
           created_date,
           date_trunc('hour', end_date) as deploy_time,
           lag(date_trunc('hour', end_date), 1) over(partition by scooter_id order by date_trunc('hour', end_date) desc) as next_deploy_time,
           cast(start_date as timestamp) as deploy_start_date,
           cast(end_date as timestamp) as deploy_end_date,
           task_location as deployed_task_location,
           st_pointfromgeohash(to_location) as deploy_point,
           st_y(ST_GeometryFromText(st_pointfromgeohash(to_location))) as deploy_latitude,
           st_x(ST_GeometryFromText(st_pointfromgeohash(to_location))) as deploy_longitude,
           date_add('hour', 24, end_date) as for_ride_in24h,
           case
            when t.priority=1 and n.created_by > 0 then 'manual'
            when t.priority=1 and n.created_by <= 0 then 'zoba'
            when t.priority=2 and t.created_by > 0 then 'hot_zone'
            when t.priority=2 and t.created_by <= 0 then 'zoba'
            when t.priority=3 and t.created_by > 0 then 'model'
            when t.priority=3 and t.created_by <= 0 then 'zoba' end as nest_type
    from martimain.temporary_tasks t
    LEFT JOIN (
        SELECT unnested_task_id, created_by
        FROM read_replica.public.mobile_service_deploy_nests
        CROSS JOIN UNNEST(task_ids) AS t(unnested_task_id)
    ) n ON t.id = n.unnested_task_id
    left join martimain.scooters on scooters.id = t.scooter_id and scooters.snapshot_date = current_date - interval '1' day
    left join martimain.scooter_body_versions as sbv on sbv.id = scooters.scooter_body_version_id
         and sbv.snapshot_date = date'2024-07-21'
         and sbv.is_active = true
    where t.snapshot_date = current_date - interval '1' day
      and end_date >= cast('2024-01-01' as date)
      and task_type = 16
      and status = 5
),

geofence_names as (
    select
        geofence_group,
        name
    from (
        select
            geofence_group,
            name,
            row_number() over (partition by geofence_group order by is_default desc) as rn
        from
            martimain.geofences
        where
            snapshot_date = current_date - interval '1' day
    )
    where
        rn=1
),

all_deploy_tasks as (
    select d.*, r.deploy_type,
           g.name as geofence_name,
           geo_to_h3_address(deploy_latitude, deploy_longitude, 9) as h3_index
    from deploy_tasks d
    left join no_duplicate_rebalance_tasks r on d.task_id = r.dp_task_id
    left join geofence_names g on d.geofence_group = g.geofence_group
)



,windows_limits as (
    select *, date_diff('day', window_start, window_end) + 1 as day_count
    from (
        select  
 adt.*,
 greatest(adt.deploy_end_date + interval '24' hour, date('{start_date}')) as window_start,
 least(coalesce(adt.next_deploy_time, now()), date('{end_date}')) as window_end
 from all_deploy_tasks adt 
    )
)


, geofence_based_total as (select g.name geofence_name, count(distinct scooter_id) total_scooter
from windows_limits wl 
inner join martimain.scooters s on s.id = wl.scooter_id and s.status_id in (1,2,16) and s.snapshot_date >= date('{start_date}') and s.snapshot_date <= date('{end_date}')
left join geofence_names g on s.geofence_group = g.geofence_group 
group by 1)


, deployed_total as (
select geofence_name, count(distinct scooter_id) deployed 
from  all_deploy_tasks d
where deploy_end_date >= date('{start_date}') and deploy_end_date <= date('{end_date}')
group by 1)


, final_deploys as (select gbt.geofence_name , total_scooter - deployed saha
from geofence_based_total gbt 
left join deployed_total dt on gbt.geofence_name = dt.geofence_name
)

, geofence_based_total_raw as (select distinct scooter_id scooter_id
from windows_limits wl 
inner join martimain.scooters s on s.id = wl.scooter_id and s.status_id in (1,2,16) and s.snapshot_date >= date('2025-05-01') and s.snapshot_date <= date('2025-05-09'))

, deployed_total_raw as (
select distinct scooter_id deployed 
from  all_deploy_tasks d
where deploy_end_date >= date('2025-05-01') and deploy_end_date <= date('2025-05-09'))

, final_deploys_raw as (select *
from geofence_based_total_raw gbt 
where scooter_id not in (select * from deployed_total_raw )
)

,deploy_to_ride_count_raw as (
    select d.task_id,
           max(d.scooter_id) as scooter_id,
           max(d.scooter_body_version_name) as scooter_body_version_name,
           max(d.geofence_group) as geofence_group,
           max(d.geofence_name) as geofence_name,
           max(cast(d.deploy_time as timestamp)) as deploy_time,
           max(d.next_deploy_time) as next_deploy_time,
           max(d.deploy_end_date) as deploy_end_date,
           max(d.deployed_task_location) as deployed_task_location,
           max(d.deploy_point) as deploy_point,
           max(d.deploy_latitude) as deploy_latitude,
           max(d.deploy_longitude) as deploy_longitude,
           max(case when d.deploy_type is null then 'depodan sahaya' else d.deploy_type end) as deploy_type,
           max(d.nest_type) as nest_type,
           max(d.h3_index) as h3_index,
           max(d.window_start) as window_start,
           max(d.window_end) as window_end,
           max(d.day_count) as day_count,
           CAST(SUM(CASE WHEN r.charged_price IS NULL THEN 0 ELSE r.charged_price END) AS double precision) as revenue_in_window,
           CAST(SUM(CASE WHEN r.actual_price IS NULL THEN 0 ELSE r.actual_price END) AS double precision) as gross_revenue_in_window,
           CAST(count(distinct CASE WHEN r.start_time IS NOT NULL THEN r.id END) AS double precision) as ride_count_in_window,
           CAST(SUM(CASE WHEN r.start_time IS NULL THEN 0 ELSE date_diff('minute', r.start_time, r.end_time) END) AS double precision) as ride_duration_in_window
    from windows_limits d
    left join martimain.temporary_rides r on d.scooter_id = r.scooter_id
         and r.start_time >= d.window_start
         and r.start_time <= d.window_end
         and r.snapshot_date = current_date - interval '1' day
         where coalesce(next_deploy_time, now()) >= d.window_start and d.deploy_end_date + interval '24' hour <= d.window_end
    group by d.task_id
)


, deneme as (

select 
d.geofence_name,
count(distinct case when ride_count_in_window = 0 or ride_count_in_window is null then scooter_id else NULL end) as yatan_arac
 from deploy_to_ride_count_raw d
left join final_deploys s on d.geofence_name = s.geofence_name
where s.saha is not null
group by 1
)


select 
  task_id,
  scooter_id,
  scooter_body_version_name,
  d.geofence_name,
  deploy_time,
  next_deploy_time,
  deploy_end_date,
  deployed_task_location,
  deploy_point,
  deploy_latitude,
  deploy_longitude,
  nest_type,
  h3_index,
  window_start,
  window_end,
  revenue_in_window,
  gross_revenue_in_window,
  ride_count_in_window,
  ride_duration_in_window,
  revenue_in_window / day_count*1.0 as revenue_per_day,
  gross_revenue_in_window / day_count*1.0 as gross_revenue_per_day,
  ride_count_in_window / day_count*1.0 as ride_count_per_day,
  ride_duration_in_window / day_count*1.0 as ride_duration_per_day,
  day_count,
  s.saha deploy_counts
from deploy_to_ride_count_raw d
left join final_deploys s on d.geofence_name = s.geofence_name
where s.saha is not null
"""
            # Define hour windows and multipliers
            hour_windows = [24, 48, 72]
            multipliers = {24: 1, 48: 2, 72: 3}
            for hour in hour_windows:
                st.header(f"Summary ({hour} Hours)")
                # Replace interval in SQL
                sql_hour = sql.replace("+ interval '24' hour", f"+ interval '{hour}' hour")
                df = wr.athena.read_sql_query(sql_hour, database="martimain", ctas_approach=False)
                st.write(f"Query returned {len(df)} rows for {hour} hours")

                # Calculate metrics
                deploy_counts = df.groupby('geofence_name')['deploy_counts'].mean().sum()
                ride_counts = df.ride_count_per_day.sum() * multipliers[hour]
                ride_counts_total = df.ride_count_in_window.sum()
                revenue = df.revenue_per_day.sum() * multipliers[hour]
                revenue_total = df.revenue_in_window.sum()

                revenue_per_deploy = revenue / deploy_counts
                ride_per_deploy = ride_counts / deploy_counts
                revenue_per_ride = revenue / ride_counts

                # Prepare summary table
                summary_data = {
                    'Metric': [
                        'Deploy Count',
                        'Revenue',
                        'Revenue (Total)',
                        'Ride Count',
                        'Ride Count (Total)',
                        'Revenue per Deploy',
                        'Ride per Deploy',
                        'Revenue per Ride'
                    ],
                    'Value': [
                        f"{deploy_counts:,.0f}",
                        f"{revenue:,.2f}",
                        f"{revenue_total:,.2f}",
                        f"{ride_counts:,.0f}",
                        f"{ride_counts_total:,.0f}",
                        f"{revenue_per_deploy:,.2f}",
                        f"{ride_per_deploy:,.2f}",
                        f"{revenue_per_ride:,.2f}"
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                st.table(summary_df)

                # Grouped summary by geofence_name
                grouped = df.groupby('geofence_name').agg({
                    'deploy_counts': 'mean',
                    'revenue_per_day': 'sum',
                    'ride_count_per_day': 'sum'
                }).reset_index()
                grouped['revenue_per_day'] = grouped['revenue_per_day'] * multipliers[hour]
                grouped['ride_count_per_day'] = grouped['ride_count_per_day'] * multipliers[hour]
                grouped['revenue_per_deploy'] = grouped['revenue_per_day'] / grouped['deploy_counts']
                grouped['ride_per_deploy'] = grouped['ride_count_per_day'] / grouped['deploy_counts']
                grouped['revenue_per_ride'] = grouped['revenue_per_day'] / grouped['ride_count_per_day']

                grouped_summary = grouped.rename(columns={
                    'geofence_name': 'Geofence',
                    'deploy_counts': 'Deploy Count',
                    'revenue_per_day': 'Revenue',
                    'ride_count_per_day': 'Ride Count',
                    'revenue_per_deploy': 'Revenue per Deploy',
                    'ride_per_deploy': 'Ride per Deploy',
                    'revenue_per_ride': 'Revenue per Ride'
                })
                # Format numbers
                for col in ['Deploy Count', 'Revenue', 'Ride Count', 'Revenue per Deploy', 'Ride per Deploy', 'Revenue per Ride']:
                    grouped_summary[col] = grouped_summary[col].apply(lambda x: f"{x:,.2f}" if isinstance(x, float) else f"{x:,}")

                st.write(f"### Summary by Geofence ({hour} Hours)")
                st.dataframe(grouped_summary)

            # Show the last df (for 72h) as raw data
            st.dataframe(df)

# Main execution
if not st.session_state.authenticated or check_session_timeout():
    login_page()
else:
    main_dashboard()
