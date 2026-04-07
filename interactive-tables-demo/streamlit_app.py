import streamlit as st
import time
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="ER Command Center",
    page_icon=":hospital:",
    layout="wide",
)

st.title(":hospital: ER Patient Admissions — Interactive Tables Demo")
st.caption("Powered by Snowflake Interactive Tables & Interactive Warehouses | Sub-second analytics")

session = get_active_session()

INTERACTIVE_WH = "ER_INTERACTIVE_WH"
STANDARD_WH = "ER_STANDARD_WH"
TABLE = "ER_INTERACTIVE_DEMO.ER_DATA.ER_ADMISSIONS_IT"
SOURCE = "ER_INTERACTIVE_DEMO.ER_DATA.ER_ADMISSIONS_SOURCE"


def query_interactive(sql):
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(sql).to_pandas()


def exec_standard(sql):
    session.sql(f"USE WAREHOUSE {STANDARD_WH}").collect()
    return session.sql(sql).collect()


@st.cache_data(ttl=30)
def get_total_count():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(f"SELECT COUNT(*) AS CNT FROM {TABLE}").to_pandas().iloc[0]["CNT"]


@st.cache_data(ttl=30)
def get_status_counts():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"SELECT STATUS, COUNT(*) AS CNT FROM {TABLE} GROUP BY STATUS ORDER BY CNT DESC"
    ).to_pandas()


@st.cache_data(ttl=30)
def get_triage_distribution():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT TRIAGE_LEVEL, TRIAGE_LABEL, COUNT(*) AS PATIENT_COUNT,
               ROUND(AVG(WAIT_TIME_MINUTES),1) AS AVG_WAIT_MIN
        FROM {TABLE} GROUP BY TRIAGE_LEVEL, TRIAGE_LABEL ORDER BY TRIAGE_LEVEL"""
    ).to_pandas()


@st.cache_data(ttl=30)
def get_facility_stats():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT FACILITY, COUNT(*) AS ADMISSIONS,
               ROUND(AVG(WAIT_TIME_MINUTES),1) AS AVG_WAIT_MIN,
               MAX(WAIT_TIME_MINUTES) AS MAX_WAIT_MIN
        FROM {TABLE} GROUP BY FACILITY ORDER BY AVG_WAIT_MIN DESC"""
    ).to_pandas()


@st.cache_data(ttl=30)
def get_top_complaints():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT CHIEF_COMPLAINT, COUNT(*) AS OCCURRENCES,
               ROUND(AVG(WAIT_TIME_MINUTES),1) AS AVG_WAIT_MIN
        FROM {TABLE}
        GROUP BY CHIEF_COMPLAINT
        ORDER BY OCCURRENCES DESC LIMIT 10"""
    ).to_pandas()


@st.cache_data(ttl=30)
def get_arrival_modes():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT ARRIVAL_MODE, COUNT(*) AS CNT,
               ROUND(AVG(TRIAGE_LEVEL),1) AS AVG_TRIAGE
        FROM {TABLE} GROUP BY ARRIVAL_MODE ORDER BY CNT DESC"""
    ).to_pandas()


@st.cache_data(ttl=30)
def get_critical_patients():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT ADMISSION_TIME, PATIENT_AGE, PATIENT_GENDER, CHIEF_COMPLAINT,
               TRIAGE_LABEL, HEART_RATE, O2_SATURATION, STATUS, ATTENDING_PHYSICIAN, FACILITY
        FROM {TABLE} WHERE TRIAGE_LEVEL <= 2 ORDER BY ADMISSION_TIME DESC LIMIT 15"""
    ).to_pandas()


@st.cache_data(ttl=30)
def get_hourly_trend():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT DATE_TRUNC('hour', ADMISSION_TIME) AS HOUR, COUNT(*) AS ADMISSIONS
        FROM {TABLE}
        GROUP BY HOUR ORDER BY HOUR"""
    ).to_pandas()


@st.cache_data(ttl=30)
def get_recent_admissions():
    session.sql(f"USE WAREHOUSE {INTERACTIVE_WH}").collect()
    return session.sql(
        f"""SELECT ADMISSION_TIME, PATIENT_AGE, PATIENT_GENDER, CHIEF_COMPLAINT,
               TRIAGE_LABEL, STATUS, ARRIVAL_MODE, FACILITY, WAIT_TIME_MINUTES
        FROM {TABLE} ORDER BY ADMISSION_TIME DESC LIMIT 20"""
    ).to_pandas()

with st.sidebar:
    st.header("Controls")

    if st.button("Refresh Dashboard", use_container_width=True):
        st.cache_data.clear()
        st.experimental_rerun()

    st.divider()
    st.subheader("Live Insert Demo")
    st.caption("Insert new ER admissions and watch them appear")

    if st.button("Simulate New Admissions", type="primary", use_container_width=True):
        exec_standard(f"""
            INSERT INTO {SOURCE}
            SELECT
                UUID_STRING(), UUID_STRING(),
                v.age, v.gender, CURRENT_TIMESTAMP(),
                v.triage,
                CASE v.triage WHEN 1 THEN 'Resuscitation' WHEN 2 THEN 'Emergent'
                    WHEN 3 THEN 'Urgent' WHEN 4 THEN 'Less Urgent' ELSE 'Non-Urgent' END,
                v.complaint, v.dept, v.physician,
                'ER-' || LPAD(UNIFORM(1,60,RANDOM())::VARCHAR, 3, '0'),
                v.hr, v.sbp, v.dbp, v.temp, v.o2, v.arrival,
                'Waiting', v.wait_min, v.facility
            FROM (
                SELECT column1 AS complaint, column2 AS dept, column3 AS triage,
                    column4 AS physician, column5 AS age, column6 AS gender,
                    column7 AS hr, column8 AS sbp, column9 AS dbp,
                    column10 AS temp, column11 AS o2, column12 AS arrival,
                    column13 AS wait_min, column14 AS facility
                FROM VALUES
                    ('Chest Pain','Cardiology',1,'Dr. Sarah Chen',67,'Male',110,180,95,99.1,94.2,'Ambulance',2,'Metro General Hospital'),
                    ('Stroke Symptoms','Neurology',1,'Dr. Aisha Patel',72,'Female',95,210,110,98.6,96.0,'Helicopter',0,'University Health System ER'),
                    ('Shortness of Breath','Pulmonology',2,'Dr. Michael Kim',55,'Male',105,150,88,100.2,89.5,'Ambulance',8,'St. Mary Regional Medical Center'),
                    ('Fracture','Orthopedics',3,'Dr. Robert Singh',34,'Female',82,128,78,98.4,98.2,'Walk-in',42,'St. Mary Regional Medical Center'),
                    ('Fever / Infection','General',3,'Dr. Maria Gonzalez',8,'Female',130,95,60,102.8,97.0,'Walk-in',28,'Coastal Community Hospital')
            ) v
        """)
        exec_standard(f"ALTER INTERACTIVE TABLE {TABLE} REFRESH")
        st.cache_data.clear()
        st.success("5 new patients inserted + refresh triggered! Click Refresh Dashboard to see updates.")

    st.divider()
    st.markdown("""
    **How it works:**
    1. Source table receives new rows
    2. Interactive Table refreshes (TARGET_LAG = 1 min)
    3. Interactive Warehouse serves sub-second queries
    """)

total = get_total_count()
status_df = get_status_counts()

waiting = int(status_df.loc[status_df["STATUS"] == "Waiting", "CNT"].values[0]) if "Waiting" in status_df["STATUS"].values else 0
in_treatment = int(status_df.loc[status_df["STATUS"] == "In Treatment", "CNT"].values[0]) if "In Treatment" in status_df["STATUS"].values else 0
admitted = int(status_df.loc[status_df["STATUS"] == "Admitted", "CNT"].values[0]) if "Admitted" in status_df["STATUS"].values else 0
discharged = int(status_df.loc[status_df["STATUS"] == "Discharged", "CNT"].values[0]) if "Discharged" in status_df["STATUS"].values else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Admissions", f"{total:,}")
c2.metric("Waiting", f"{waiting:,}")
c3.metric("In Treatment", f"{in_treatment:,}")
c4.metric("Admitted", f"{admitted:,}")
c5.metric("Discharged", f"{discharged:,}")

tab1, tab2, tab3, tab4 = st.tabs(["Triage & Wait Times", "Facilities", "Live Feed", "Critical Patients"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Triage Distribution")
        triage_df = get_triage_distribution()
        st.bar_chart(triage_df, x="TRIAGE_LABEL", y="PATIENT_COUNT")

    with col2:
        st.subheader("Avg Wait by Triage Level (min)")
        st.bar_chart(triage_df, x="TRIAGE_LABEL", y="AVG_WAIT_MIN")

    st.subheader("Top Chief Complaints")
    complaints_df = get_top_complaints()
    st.dataframe(complaints_df, use_container_width=True)

with tab2:
    st.subheader("Facility Performance")
    facility_df = get_facility_stats()
    st.dataframe(facility_df, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Arrival Mode")
        arrival_df = get_arrival_modes()
        st.bar_chart(arrival_df, x="ARRIVAL_MODE", y="CNT")

    with col2:
        st.subheader("Hourly Admission Trend")
        hourly_df = get_hourly_trend()
        if not hourly_df.empty:
            st.area_chart(hourly_df, x="HOUR", y="ADMISSIONS")
        else:
            st.info("No admission data available")

with tab3:
    st.subheader("Most Recent Admissions")
    recent_df = get_recent_admissions()
    st.dataframe(recent_df, use_container_width=True)

with tab4:
    st.subheader("Critical Patients (Triage Level 1-2)")
    critical_df = get_critical_patients()
    st.dataframe(critical_df, use_container_width=True)
