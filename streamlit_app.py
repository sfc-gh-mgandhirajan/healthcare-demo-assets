import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Medicare Part A — Pre-payment edit dashboard",
    page_icon=":hospital:",
    layout="wide",
)

session = get_active_session()


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()


def load_dq_gate_summary():
    return run_query(
        "SELECT * FROM MEDICARE_CLAIMS_POC.DQ.V_DQ_GATE_SUMMARY ORDER BY DQ_STATUS"
    )


def load_disposition_summary():
    return run_query(
        "SELECT * FROM MEDICARE_CLAIMS_POC.GOLD.V_DISPOSITION_SUMMARY ORDER BY CLAIM_COUNT DESC"
    )


def load_edit_hit_rate():
    return run_query(
        "SELECT * FROM MEDICARE_CLAIMS_POC.GOLD.V_EDIT_HIT_RATE ORDER BY HIT_COUNT DESC"
    )


def load_quarantine_summary():
    return run_query("""
        SELECT DQ_FAIL_SNIP_CATEGORY, COUNT(*) AS CLAIM_COUNT,
               SUM(DQ_INVALID_NPI) AS NPI_ISSUES,
               SUM(DQ_INVALID_MBI) AS MBI_ISSUES,
               SUM(DQ_INVALID_TOB) AS TOB_ISSUES,
               SUM(DQ_FUTURE_DATE) AS FUTURE_DATE_ISSUES,
               SUM(DQ_DATE_INCONSISTENCY) AS DATE_CONSISTENCY_ISSUES,
               SUM(DQ_CHARGES_OUT_OF_RANGE) AS CHARGES_RANGE_ISSUES,
               SUM(DQ_MISSING_ADM_SOURCE) AS ADM_SOURCE_ISSUES,
               SUM(DQ_INVALID_ICD10) AS ICD10_ISSUES
        FROM MEDICARE_CLAIMS_POC.DQ.V_DQ_QUARANTINE
        GROUP BY DQ_FAIL_SNIP_CATEGORY
        ORDER BY CLAIM_COUNT DESC
    """)


def load_quarantine_detail():
    return run_query("""
        SELECT CLAIM_ID, DCN, PROVIDER_NPI, BENEFICIARY_HIC,
               ADMISSION_DATE, DISCHARGE_DATE, TOTAL_CHARGES, TYPE_OF_BILL,
               DQ_ISSUE_COUNT, DQ_FAIL_SNIP_CATEGORY,
               DQ_INVALID_NPI, DQ_INVALID_MBI, DQ_INVALID_TOB,
               DQ_FUTURE_DATE, DQ_DATE_INCONSISTENCY,
               DQ_CHARGES_OUT_OF_RANGE, DQ_MISSING_ADM_SOURCE, DQ_INVALID_ICD10
        FROM MEDICARE_CLAIMS_POC.DQ.V_DQ_QUARANTINE
        ORDER BY DQ_ISSUE_COUNT DESC
        LIMIT 500
    """)


def load_provider_risk():
    return run_query("""
        SELECT PROVIDER_NPI, PROVIDER_NAME, PROVIDER_TYPE, PECOS_STATUS, STATE,
               TOTAL_CLAIMS, DENIED_REJECTED_CLAIMS, DENY_REJECT_RATE,
               TOTAL_CHARGES_SUBMITTED, TOTAL_CHARGES_ACCEPTED, TOTAL_CHARGES_DENIED
        FROM MEDICARE_CLAIMS_POC.GOLD.V_PROVIDER_RISK
        ORDER BY DENY_REJECT_RATE DESC
        LIMIT 100
    """)


def load_payer_edit_breakdown():
    return run_query(
        "SELECT * FROM MEDICARE_CLAIMS_POC.GOLD.V_PAYER_EDIT_BREAKDOWN"
    )


def load_adjudication_by_drg():
    return run_query("""
        SELECT DRG_CODE, DISPOSITION, COUNT(*) AS CLAIM_COUNT
        FROM MEDICARE_CLAIMS_POC.GOLD.CLAIM_ADJUDICATION
        WHERE DRG_CODE IS NOT NULL
        GROUP BY DRG_CODE, DISPOSITION
        ORDER BY CLAIM_COUNT DESC
        LIMIT 200
    """)


st.title("Medicare Part A pre-payment edit dashboard")
st.caption("Near real-time claims DQ monitoring and edit adjudication results")

dq_gate = load_dq_gate_summary()
disposition = load_disposition_summary()
edit_hits = load_edit_hit_rate()

total_claims = int(dq_gate["CLAIM_COUNT"].sum()) if not dq_gate.empty else 0
dq_pass_row = dq_gate[dq_gate["DQ_STATUS"] == "DQ_PASS"]
dq_fail_row = dq_gate[dq_gate["DQ_STATUS"] == "DQ_FAIL"]
pass_count = int(dq_pass_row["CLAIM_COUNT"].iloc[0]) if not dq_pass_row.empty else 0
fail_count = int(dq_fail_row["CLAIM_COUNT"].iloc[0]) if not dq_fail_row.empty else 0
pass_pct = float(dq_pass_row["PERCENTAGE"].iloc[0]) if not dq_pass_row.empty else 0
fail_pct = float(dq_fail_row["PERCENTAGE"].iloc[0]) if not dq_fail_row.empty else 0

accept_count = 0
deny_count = 0
if not disposition.empty:
    accept_row = disposition[disposition["DISPOSITION"] == "ACCEPT"]
    deny_row = disposition[disposition["DISPOSITION"] == "DENY"]
    accept_count = int(accept_row["CLAIM_COUNT"].iloc[0]) if not accept_row.empty else 0
    deny_count = int(deny_row["CLAIM_COUNT"].iloc[0]) if not deny_row.empty else 0

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Total claims", f"{total_claims:,}")
kpi2.metric("DQ pass", f"{pass_count:,}", f"{pass_pct:.1f}%")
kpi3.metric("DQ fail", f"{fail_count:,}", f"{fail_pct:.1f}%", delta_color="inverse")
kpi4.metric("Accepted", f"{accept_count:,}")
kpi5.metric("Denied", f"{deny_count:,}")

st.divider()

tab_dq, tab_edits, tab_disposition, tab_providers, tab_quarantine = st.tabs([
    "DQ gate",
    "Edit rules",
    "Disposition",
    "Provider risk",
    "Quarantine detail",
])

with tab_dq:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("DQ gate pass / fail")
        if not dq_gate.empty:
            color_map = {"DQ_PASS": "#4CAF50", "DQ_FAIL": "#F44336"}
            chart = (
                alt.Chart(dq_gate)
                .mark_arc(innerRadius=60)
                .encode(
                    theta=alt.Theta("CLAIM_COUNT:Q"),
                    color=alt.Color(
                        "DQ_STATUS:N",
                        scale=alt.Scale(
                            domain=list(color_map.keys()),
                            range=list(color_map.values()),
                        ),
                        legend=alt.Legend(title="Status"),
                    ),
                    tooltip=["DQ_STATUS", "CLAIM_COUNT", "PERCENTAGE"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)

    with col2:
        st.subheader("SNIP category breakdown")
        snip_data = load_quarantine_summary()
        if not snip_data.empty:
            snip_colors = {
                "SNIP_1_2": "#FF9800",
                "SNIP_3_4": "#2196F3",
                "SNIP_5_6": "#9C27B0",
            }
            chart = (
                alt.Chart(snip_data)
                .mark_bar()
                .encode(
                    x=alt.X("CLAIM_COUNT:Q", title="Failed claims"),
                    y=alt.Y(
                        "DQ_FAIL_SNIP_CATEGORY:N",
                        title="SNIP category",
                        sort="-x",
                    ),
                    color=alt.Color(
                        "DQ_FAIL_SNIP_CATEGORY:N",
                        scale=alt.Scale(
                            domain=list(snip_colors.keys()),
                            range=list(snip_colors.values()),
                        ),
                        legend=None,
                    ),
                    tooltip=["DQ_FAIL_SNIP_CATEGORY", "CLAIM_COUNT"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)

    st.subheader("DQ issue breakdown by SNIP category")
    snip_data = load_quarantine_summary()
    if not snip_data.empty:
        issue_cols = [
            "NPI_ISSUES", "MBI_ISSUES", "TOB_ISSUES", "FUTURE_DATE_ISSUES",
            "DATE_CONSISTENCY_ISSUES", "CHARGES_RANGE_ISSUES",
            "ADM_SOURCE_ISSUES", "ICD10_ISSUES",
        ]
        melted = snip_data.melt(
            id_vars=["DQ_FAIL_SNIP_CATEGORY"],
            value_vars=issue_cols,
            var_name="Issue type",
            value_name="Count",
        )
        melted = melted[melted["Count"] > 0]
        melted["Issue type"] = melted["Issue type"].str.replace("_ISSUES", "").str.replace("_", " ").str.title()

        chart = (
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("Count:Q", title="Issue count"),
                y=alt.Y("Issue type:N", sort="-x", title=None),
                color=alt.Color("DQ_FAIL_SNIP_CATEGORY:N", title="SNIP category"),
                tooltip=["DQ_FAIL_SNIP_CATEGORY", "Issue type", "Count"],
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)

with tab_edits:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Edit rule hit rates")
        if not edit_hits.empty:
            chart = (
                alt.Chart(edit_hits)
                .mark_bar()
                .encode(
                    x=alt.X("HIT_COUNT:Q", title="Hit count"),
                    y=alt.Y("EDIT_NAME:N", sort="-x", title=None),
                    color=alt.Color(
                        "SEVERITY:N",
                        scale=alt.Scale(
                            domain=["DENY", "REJECT", "FLAG"],
                            range=["#F44336", "#FF9800", "#FFC107"],
                        ),
                        legend=alt.Legend(title="Severity"),
                    ),
                    tooltip=["EDIT_CODE", "EDIT_NAME", "SEVERITY", "HIT_COUNT", "HIT_RATE_PCT"],
                )
                .properties(height=400)
            )
            st.altair_chart(chart, use_container_width=True)

    with col2:
        st.subheader("Payer-specific edits (MUE / NCCI / Frequency)")
        payer_data = load_payer_edit_breakdown()
        if not payer_data.empty:
            chart = (
                alt.Chart(payer_data)
                .mark_bar()
                .encode(
                    x=alt.X("EDIT_NAME:N", title=None),
                    y=alt.Y("HIT_COUNT:Q", title="Hit count"),
                    color=alt.Color("EDIT_NAME:N", legend=None),
                    tooltip=["EDIT_CODE", "EDIT_NAME", "HIT_COUNT", "DISTINCT_CLAIMS"],
                )
                .properties(height=400)
            )
            st.altair_chart(chart, use_container_width=True)

    st.subheader("Edit rule detail")
    if not edit_hits.empty:
        st.dataframe(edit_hits, use_container_width=True)

with tab_disposition:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Claim disposition")
        if not disposition.empty:
            disp_colors = {
                "ACCEPT": "#4CAF50",
                "DENY": "#F44336",
                "REJECT": "#FF9800",
                "FLAG": "#FFC107",
                "DQ_REJECT": "#9C27B0",
            }
            chart = (
                alt.Chart(disposition)
                .mark_arc(innerRadius=60)
                .encode(
                    theta=alt.Theta("CLAIM_COUNT:Q"),
                    color=alt.Color(
                        "DISPOSITION:N",
                        scale=alt.Scale(
                            domain=list(disp_colors.keys()),
                            range=list(disp_colors.values()),
                        ),
                        legend=alt.Legend(title="Disposition"),
                    ),
                    tooltip=["DISPOSITION", "CLAIM_COUNT", "PERCENTAGE"],
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)

    with col2:
        st.subheader("Disposition breakdown")
        if not disposition.empty:
            disp_display = disposition.copy()
            disp_display["TOTAL_CHARGES"] = disp_display["TOTAL_CHARGES"].apply(lambda x: f"${x:,.2f}")
            disp_display["AVG_CHARGES"] = disp_display["AVG_CHARGES"].apply(lambda x: f"${x:,.2f}")
            disp_display["PERCENTAGE"] = disp_display["PERCENTAGE"].apply(lambda x: f"{x:.1f}%")
            st.dataframe(disp_display, use_container_width=True)

    st.subheader("Top DRG codes by disposition")
    drg_data = load_adjudication_by_drg()
    if not drg_data.empty:
        top_drgs = drg_data.groupby("DRG_CODE")["CLAIM_COUNT"].sum().nlargest(15).index.tolist()
        filtered = drg_data[drg_data["DRG_CODE"].isin(top_drgs)]
        chart = (
            alt.Chart(filtered)
            .mark_bar()
            .encode(
                x=alt.X("CLAIM_COUNT:Q", title="Claim count"),
                y=alt.Y("DRG_CODE:N", sort="-x", title="DRG code"),
                color=alt.Color(
                    "DISPOSITION:N",
                    scale=alt.Scale(
                        domain=["ACCEPT", "DENY", "REJECT", "FLAG", "DQ_REJECT"],
                        range=["#4CAF50", "#F44336", "#FF9800", "#FFC107", "#9C27B0"],
                    ),
                ),
                tooltip=["DRG_CODE", "DISPOSITION", "CLAIM_COUNT"],
            )
            .properties(height=450)
        )
        st.altair_chart(chart, use_container_width=True)

with tab_providers:
    provider_data = load_provider_risk()
    if not provider_data.empty:
        m1, m2, m3 = st.columns(3)
        avg_deny_rate = float(provider_data["DENY_REJECT_RATE"].mean())
        max_deny_rate = float(provider_data["DENY_REJECT_RATE"].max())
        total_denied_charges = float(provider_data["TOTAL_CHARGES_DENIED"].sum())
        m1.metric("Avg deny/reject rate", f"{avg_deny_rate:.1f}%")
        m2.metric("Max deny/reject rate", f"{max_deny_rate:.1f}%")
        m3.metric("Total denied charges", f"${total_denied_charges:,.0f}")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Deny/reject rate distribution")
            chart = (
                alt.Chart(provider_data)
                .mark_bar()
                .encode(
                    x=alt.X("DENY_REJECT_RATE:Q", bin=alt.Bin(maxbins=20), title="Deny/reject rate %"),
                    y=alt.Y("count()", title="Provider count"),
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)

        with col2:
            st.subheader("Provider risk by type")
            type_summary = (
                provider_data.groupby("PROVIDER_TYPE")
                .agg({"DENY_REJECT_RATE": "mean", "PROVIDER_NPI": "count"})
                .reset_index()
                .rename(columns={"PROVIDER_NPI": "PROVIDER_COUNT", "DENY_REJECT_RATE": "AVG_DENY_RATE"})
            )
            chart = (
                alt.Chart(type_summary)
                .mark_bar()
                .encode(
                    x=alt.X("AVG_DENY_RATE:Q", title="Avg deny/reject rate %"),
                    y=alt.Y("PROVIDER_TYPE:N", sort="-x", title=None),
                    tooltip=["PROVIDER_TYPE", "AVG_DENY_RATE", "PROVIDER_COUNT"],
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)

        st.subheader("Top 100 high-risk providers")
        prov_display = provider_data.copy()
        for c in ["TOTAL_CHARGES_SUBMITTED", "TOTAL_CHARGES_ACCEPTED", "TOTAL_CHARGES_DENIED"]:
            prov_display[c] = prov_display[c].apply(lambda x: f"${x:,.0f}")
        prov_display["DENY_REJECT_RATE"] = prov_display["DENY_REJECT_RATE"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(prov_display, use_container_width=True)

with tab_quarantine:
    quarantine = load_quarantine_detail()
    if not quarantine.empty:
        with st.sidebar:
            st.header("Quarantine filters")
            snip_filter = st.multiselect(
                "SNIP category",
                options=quarantine["DQ_FAIL_SNIP_CATEGORY"].unique().tolist(),
                default=quarantine["DQ_FAIL_SNIP_CATEGORY"].unique().tolist(),
            )
            min_issues = st.slider("Minimum issue count", 1, int(quarantine["DQ_ISSUE_COUNT"].max()), 1)

        filtered = quarantine[
            (quarantine["DQ_FAIL_SNIP_CATEGORY"].isin(snip_filter))
            & (quarantine["DQ_ISSUE_COUNT"] >= min_issues)
        ]
        st.caption(f"Showing {len(filtered):,} of {len(quarantine):,} quarantined claims")

        q_display = filtered.copy()
        q_display["TOTAL_CHARGES"] = q_display["TOTAL_CHARGES"].apply(lambda x: f"${x:,.2f}")
        st.dataframe(q_display, use_container_width=True)
