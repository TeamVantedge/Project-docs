import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import call_function, lit, col
import json
import re
import pandas as pd

session = get_active_session()
st.set_page_config(page_title="Resource Matcher | 5A Plan", layout="wide")
st.markdown("""
    <style>
    .block-container {padding-top: 1.5rem; padding-bottom: 1rem;}
    div[data-testid="stMetric"] {background: linear-gradient(135deg, #f8f9fc 0%, #eef1f8 100%); border: 1px solid #e2e6ef; border-radius: 10px; padding: 12px 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);}
    div[data-testid="stMetric"] label {font-size: 0.78rem; color: #5a6784;}
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {font-size: 1.3rem; color: #1a2744;}
    div[data-testid="stExpander"] {background: #ffffff; border: 1px solid #e2e6ef; border-radius: 10px; margin-bottom: 0.75rem; box-shadow: 0 1px 4px rgba(0,0,0,0.04);}
    .team-header {background: linear-gradient(135deg, #1a2744 0%, #2d4a7a 100%); color: white; padding: 1.2rem 1.5rem; border-radius: 10px; margin-bottom: 1.2rem;}
    .team-header h2 {color: white; margin: 0; font-size: 1.4rem;}
    .team-header p {color: #b8c9e8; margin: 0.3rem 0 0 0; font-size: 0.9rem;}
    .step-badge {display: inline-block; background: #2d4a7a; color: white; padding: 3px 12px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; margin-bottom: 0.4rem; letter-spacing: 0.5px;}
    .skill-pill {display: inline-block; background: linear-gradient(135deg, #e8f4e8 0%, #d4edda 100%); color: #1a5928; padding: 5px 14px; border-radius: 20px; margin: 3px 4px; font-size: 0.82rem; font-weight: 500; border: 1px solid #b7dfc0;}
    .ai-pill {display: inline-block; background: linear-gradient(135deg, #e8f0fe 0%, #d4e4fc 100%); color: #1a4a8a; padding: 5px 14px; border-radius: 20px; margin: 3px 4px; font-size: 0.82rem; font-weight: 500; border: 1px solid #b0cff5;}
    .companion-pill {display: inline-block; background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%); color: #e65100; padding: 5px 14px; border-radius: 20px; margin: 3px 4px; font-size: 0.82rem; font-weight: 500; border: 1px solid #ffcc80;}
    .missing-skill-pill {display: inline-block; background: linear-gradient(135deg, #ffeaea 0%, #ffd4d4 100%); color: #c62828; padding: 5px 14px; border-radius: 20px; margin: 3px 4px; font-size: 0.82rem; font-weight: 500; border: 1px solid #ef9a9a;}
    .profile-section {background: #f8f9fc; border-radius: 8px; padding: 10px 14px; margin: 6px 0; border-left: 3px solid #2d4a7a;}
    .ts-total-bar {background: #1a8754; color: white; padding: 6px 12px; border-radius: 6px; margin-top: 6px; display: flex; justify-content: space-between; font-weight: 700; font-size: 0.85rem;}
    .prof-expert {color: #1a8754; font-weight: 700;} .prof-advanced {color: #2d4a7a; font-weight: 600;} .prof-intermediate {color: #d4910a; font-weight: 600;} .prof-beginner {color: #6c757d; font-weight: 600;}
    .approval-box {border: 2px solid #e2e6ef; border-radius: 12px; padding: 1.5rem; margin: 1.5rem 0; background: linear-gradient(135deg, #f8f9fc 0%, #fff 100%); text-align: center;}
    .approval-box h3 {color: #1a2744; margin-bottom: 0.5rem;} .approval-box p {color: #5a6784; margin-bottom: 1rem;}
    .rejected-banner {background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%); border: 1px solid #ffcc80; border-radius: 8px; padding: 8px 14px; margin-bottom: 1rem; font-size: 0.85rem; color: #e65100;}
    .margin-section {background: linear-gradient(135deg, #f0f4ff 0%, #e8ecf8 100%); border-radius: 8px; padding: 10px 14px; margin: 8px 0; border-left: 3px solid #1a8754;}
    .dash-card {background: linear-gradient(135deg, #ffffff 0%, #f8f9fc 100%); border: 1px solid #e2e6ef; border-radius: 12px; padding: 1.2rem; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.04);}
    .dash-card h1 {margin: 0; font-size: 2.2rem;} .dash-card p {margin: 0.3rem 0 0 0; font-size: 0.85rem; color: #5a6784;}
    .dash-approved {border-left: 4px solid #1a8754;} .dash-rejected {border-left: 4px solid #c62828;} .dash-total {border-left: 4px solid #2d4a7a;} .dash-rate {border-left: 4px solid #ffb300;}
    .timeline-approved {background: #e8f4e8; border-left: 4px solid #1a8754; border-radius: 8px; padding: 12px 16px; margin-bottom: 10px;}
    .timeline-rejected {background: #ffeaea; border-left: 4px solid #c62828; border-radius: 8px; padding: 12px 16px; margin-bottom: 10px;}
    .selected-summary {background: linear-gradient(135deg, #e8f4e8 0%, #d4edda 100%); border: 2px solid #1a8754; border-radius: 10px; padding: 12px 16px; margin: 10px 0;}
    section[data-testid="stSidebar"] > div {background: linear-gradient(180deg, #1a2744 0%, #243656 100%);}
    section[data-testid="stSidebar"] label, section[data-testid="stSidebar"] .stMarkdown p, section[data-testid="stSidebar"] .stMarkdown li, section[data-testid="stSidebar"] .stMarkdown h1, section[data-testid="stSidebar"] .stMarkdown h2, section[data-testid="stSidebar"] .stMarkdown h3, section[data-testid="stSidebar"] span {color: #e0e7f1 !important;}
    section[data-testid="stSidebar"] .stMarkdown table th, section[data-testid="stSidebar"] .stMarkdown table td {color: #c8d3e3 !important; border-color: #3a5478 !important;}
    section[data-testid="stSidebar"] hr {border-color: #3a5478 !important;}
    div[data-testid="stFileUploader"] {border: 2px dashed #b0c4de; border-radius: 10px; padding: 8px;}
    .stButton > button[kind="primary"] {background: linear-gradient(135deg, #1a8754 0%, #22a06b 100%); border: none; border-radius: 8px; padding: 0.6rem 1.5rem; font-weight: 600;}
    .stButton > button[kind="secondary"] {background: linear-gradient(135deg, #2d4a7a 0%, #3b6cb5 100%); color: white; border: none; border-radius: 8px; padding: 0.6rem 1.5rem; font-weight: 600;}
    </style>""", unsafe_allow_html=True)

col_logo, col_title = st.columns([0.07, 0.93])
with col_logo:
    st.markdown("<div style='font-size:2.5rem; margin-top:4px;'>&#9878;</div>", unsafe_allow_html=True)
with col_title:
    st.markdown("<h1 style='margin:0; color:#1a2744;'>Resource Matcher &mdash; Find the best team for your engagement</p></h1><p style='margin:0; color:#5a6784; font-size:1rem;'>", unsafe_allow_html=True)
st.markdown("<div style='height:4px; background:linear-gradient(90deg,#1a2744,#2d4a7a,#3b6cb5,#e2e6ef); border-radius:2px; margin-bottom:1.2rem;'></div>", unsafe_allow_html=True)

ROLE_MAP = {"ARCHITECT": ["Associate Solutions Architect", "Solutions Architect", "Sr. Solutions Architect", "Principle Solutions Architect"], "SR_CONSULTANT": ["Lead Engineer", "Sr. Lead Engineer"], "JR_CONSULTANT": ["Software Engineer", "Sr. Software Engineer"], "PM": ["Project Manager", "Sr. Project Manager"]}
PROFICIENCY_LEVELS = ["Beginner", "Intermediate", "Advanced", "Expert"]
PROF_RANK = {"Beginner": 1, "Intermediate": 2, "Advanced": 3, "Expert": 4}
QTY_OPTIONS = list(range(0, 11))
SKILL_COMPANIONS = {"Fivetran": [("dbt", "Advanced")], "dbt": [("Fivetran", "Intermediate")], "Snowflake": [("SQL", "Advanced")], "Databricks": [("Apache Spark", "Advanced")], "GCP": [("BigQuery", "Intermediate")], "Azure": [("ADF", "Intermediate")], "Machine Learning": [("Python", "Advanced")], "Deep Learning": [("Python", "Advanced")]}

@st.cache_data
def load_skills():
    rows = session.sql("SELECT DISTINCT SKILL_NAME FROM KIPI_RM_DB.RM_SCHEMA.SKILLS_DETAILS ORDER BY SKILL_NAME").collect()
    return [r["SKILL_NAME"] for r in rows]

available_skills = load_skills()
for key, default in [("skill_prof_df", pd.DataFrame(columns=["Skill", "Required Proficiency"])), ("plan_analyzed", False), ("team_result", None), ("all_candidates", None), ("approval_status", None), ("valid_skills", None), ("uploader_key", 0), ("rejected_employees", set()), ("db_updates", []), ("approved_project", ""), ("approved_client", ""), ("feedback_summaries", {})]:
    if key not in st.session_state:
        st.session_state[key] = default

def extract_file_text(uploaded):
    name = uploaded.name.lower()
    if name.endswith(".txt"): return uploaded.read().decode("utf-8")
    elif name.endswith(".csv"): return pd.read_csv(uploaded).to_string(index=False)
    elif name.endswith(".pdf"):
        try:
            from pypdf import PdfReader
            return "\n".join(page.extract_text() or "" for page in PdfReader(uploaded).pages)
        except Exception: st.error("PDF parsing unavailable."); return None
    return None

SKILL_ALIASES = {
    "Snowpark": ["snow park", "snow-park"],
    "Snowpipe": ["snow pipe", "snow-pipe"],
    "Cortex AI": ["cortexai", "cortex-ai", "snowflake cortex", "cortex"],
    "Dynamic Tables": ["dynamictables", "dynamic-tables"],
    "Tasks and Streams": ["tasks & streams", "tasksandstreams", "snowflake tasks", "snowflake streams"],
    "Native Apps": ["nativeapps", "native-apps", "snowflake native app"],
    "Apache Iceberg": ["iceberg", "apacheiceberg"],
    "Data Warehousing": ["datawarehousing", "data-warehousing", "dwh", "data warehouse"],
    "Data Governance": ["datagovernance", "data-governance"],
    "Data Quality": ["dataquality", "data-quality", "dq"],
    "REST API": ["restapi", "rest-api", "rest api integration"],
    "NoSQL": ["no sql", "no-sql", "nosql databases"],    
    "Power BI": ["powerbi", "power-bi", "pbi"],
    "Snowflake": ["snow flake"],
    "Machine Learning": ["ml", "machinelearning", "machine-learning"],
    "Deep Learning": ["dl", "deeplearning", "deep-learning"],
    "Apache Spark": ["apachespark", "apache-spark", "pyspark"],
    "Data Modelling": ["datamodelling", "data-modelling", "datamodeling", "data-modeling", "data modeling"],
    "Azure": ["ms azure", "microsoft azure"],
    "ADF": ["azure data factory", "azuredatafactory"],
    "BigQuery": ["big query", "big-query", "bq"],
    "Databricks": ["data bricks", "data-bricks"],
    "Fivetran": ["five tran", "five-tran"],
    "Terraform": ["terra form", "terra-form"],
    "Kubernetes": ["k8s"],
    "JavaScript": ["java script", "java-script", "js"],
    "TypeScript": ["type script", "type-script", "ts"],
    "PostgreSQL": ["postgres", "postgre sql"],
    "MongoDB": ["mongo db", "mongo-db", "mongo"],
    "Tableau": ["tableua"],
    "Python": ["py"],
    "Docker": ["docker container", "containerization"],
    "CI/CD": ["cicd", "ci cd", "ci-cd"],
    "DevOps": ["dev ops", "dev-ops"],
    "NoSQL": ["no sql", "no-sql", "nosql databases"],
    "ETL": ["extract transform load"],
    "ELT": ["extract load transform"],
    "dbt": ["data build tool"],
    "SQL": ["structured query language"],
    "AWS": ["amazon web services"],
    "GCP": ["google cloud", "google cloud platform"],
}

def extract_skills_from_text(plan_text, skills_list):
    plan_lower = plan_text.lower()
    plan_normalized = re.sub(r"[\s\-_./]", "", plan_lower)
    found = []
    for skill in skills_list:
        skill_lower = skill.lower()
        if re.search(r"(?<![a-zA-Z])" + re.escape(skill_lower) + r"(?![a-zA-Z])", plan_lower):
            found.append(skill)
            continue
        skill_normalized = re.sub(r"[\s\-_./]", "", skill_lower)
        if len(skill_normalized) > 2 and skill_normalized in plan_normalized:
            found.append(skill)
            continue
        aliases = SKILL_ALIASES.get(skill, [])
        for alias in aliases:
            alias_lower = alias.lower()
            if re.search(r"(?<![a-zA-Z])" + re.escape(alias_lower) + r"(?![a-zA-Z])", plan_lower):
                found.append(skill)
                break
            alias_normalized = re.sub(r"[\s\-_./]", "", alias_lower)
            if len(alias_normalized) > 2 and alias_normalized in plan_normalized:
                found.append(skill)
                break
    return found	

def detect_proficiency_hint(plan_text, skill):
    plan_lower = plan_text.lower()
    idx = plan_lower.find(skill.lower())
    if idx == -1: return "Intermediate"
    context = plan_lower[max(0, idx - 120):min(len(plan_lower), idx + len(skill) + 120)]
    for kw in ["expert", "deep expertise", "mastery", "extensive", "highly experienced"]:
        if kw in context: return "Expert"
    for kw in ["advanced", "proficient", "solid", "strong", "experienced", "hands-on"]:
        if kw in context: return "Advanced"
    for kw in ["basic", "beginner", "foundational", "introductory", "awareness"]:
        if kw in context: return "Beginner"
    return "Intermediate"

def render_skill_pills_with_prof(df_skills, companion_names=None):
    if companion_names is None: companion_names = set()
    html = ""
    for _, r in df_skills.iterrows():
        prof = r["Required Proficiency"]; css = f"prof-{prof.lower()}"
        pill_css = "companion-pill" if r["Skill"] in companion_names else "skill-pill"
        html += f"<span class='{pill_css}'>{r['Skill']} <span class='{css}'>({prof})</span></span>"
    st.markdown(html, unsafe_allow_html=True)

def get_feedback_summaries(emp_ids):
    if not emp_ids:
        return {}
    ids_sql = ", ".join([str(int(eid)) for eid in emp_ids])
    try:
        result = session.sql(f"""
            WITH remarks AS (
                SELECT EMPLOYEE_ID, REMARK, FEEDBACK_DATE,
                       SNOWFLAKE.CORTEX.SENTIMENT(REMARK) AS sentiment_score
                FROM KIPI_RM_DB.RM_SCHEMA.FEEDBACK_DETAILS
                WHERE EMPLOYEE_ID IN ({ids_sql})
            ),
            agg AS (
                SELECT EMPLOYEE_ID,
                       LISTAGG(REMARK, '. ') WITHIN GROUP (ORDER BY FEEDBACK_DATE DESC) AS ALL_REMARKS,
                       LISTAGG(CASE WHEN sentiment_score < -0.3 THEN REMARK END, ' | ') WITHIN GROUP (ORDER BY FEEDBACK_DATE DESC) AS NEGATIVE_REMARKS
                FROM remarks
                GROUP BY EMPLOYEE_ID
            )
            SELECT EMPLOYEE_ID,
                   SNOWFLAKE.CORTEX.COMPLETE(
                       'llama3.1-70b',
                       'Summarize the following employee feedback into exactly one short sentence (max 20 words). Return ONLY the summary sentence, nothing else: ' || ALL_REMARKS
                   ) AS FEEDBACK_SUMMARY,
                   COALESCE(NEGATIVE_REMARKS, '') AS NEGATIVE_REMARKS
            FROM agg
        """).to_pandas()
        summaries = {}
        for _, row in result.iterrows():
            eid = int(row["EMPLOYEE_ID"])
            summaries[eid] = {
                "summary": str(row["FEEDBACK_SUMMARY"]).strip(),
                "negatives": str(row["NEGATIVE_REMARKS"]).strip()
            }
        return summaries
    except Exception:
        return {}

def build_email_body(team_df, skills_df, section_cfg):
    lines = ["TEAM ALLOCATION - APPROVED", "=" * 50, "", "REQUIRED SKILLS:"]
    for _, r in skills_df.iterrows(): lines.append(f"  - {r['Skill']} ({r['Required Proficiency']})")
    lines += ["", "ALLOCATED TEAM:", "-" * 50]
    for cat_key, cat_count, cat_title, _ in section_cfg:
        if cat_count == 0: continue
        cat_members = team_df[team_df["ROLE_CATEGORY"] == cat_key]
        if cat_members.empty: continue
        lines.append(f"\n{cat_title} ({len(cat_members)}):")
        for _, row in cat_members.iterrows():
            lines += [f"  Name       : {row['EMPLOYEE_NAME']}", f"  Designation: {row['DESIGNATION']}", f"  Department : {row['DEPARTMENT']}", f"  BU         : {row['BUSINESS_UNIT']}", f"  Location   : {row['LOCATION']}", f"  Experience : {row['TOTAL_EXPERIENCE_YEARS']} yrs", f"  Score      : {row['MATCH_SCORE']}/100", f"  Prof. Met  : {int(row['PROFICIENCY_MET'])}/{len(skills_df)}", f"  Skills     : {row['MATCHING_SKILLS']}", f"  Certs      : {row['CERTIFICATIONS']}"]
            if row["ON_BENCH"] == 1: lines.append(f"  Bench      : Available - {row['AVAILABILITY']}")
            lines.append("")
    lines += ["-" * 50, f"Total Positions: {len(team_df)} / {sum(c for _, c, _, _ in section_cfg)}", f"Avg Match Score: {team_df['MATCH_SCORE'].mean():.1f} / 100", f"On Bench       : {int(team_df['ON_BENCH'].sum())}", "", "This allocation was approved via the 5A Resource Matcher application."]
    return "\n".join(lines)

def log_allocation_action(action_type, project_name, client_name, team_df, skills_df, section_cfg, rejection_reason=None):
    proj = (project_name or "").replace("'", "''"); client = (client_name or "").replace("'", "''")
    skills_str = ", ".join(skills_df["Skill"].tolist()) if skills_df is not None and not skills_df.empty else "-"
    team_size = len(team_df) if team_df is not None and not team_df.empty else 0
    positions = sum(c for _, c, _, _ in section_cfg) if section_cfg else 0
    avg_score = round(team_df["MATCH_SCORE"].mean(), 1) if team_df is not None and not team_df.empty else 0
    bench_count = int(team_df["ON_BENCH"].sum()) if team_df is not None and not team_df.empty else 0
    arch_c = len(team_df[team_df["ROLE_CATEGORY"] == "ARCHITECT"]) if team_df is not None and not team_df.empty else 0
    sr_c = len(team_df[team_df["ROLE_CATEGORY"] == "SR_CONSULTANT"]) if team_df is not None and not team_df.empty else 0
    jr_c = len(team_df[team_df["ROLE_CATEGORY"] == "JR_CONSULTANT"]) if team_df is not None and not team_df.empty else 0
    pm_c = len(team_df[team_df["ROLE_CATEGORY"] == "PM"]) if team_df is not None and not team_df.empty else 0
    members = ", ".join(team_df["EMPLOYEE_NAME"].tolist()) if team_df is not None and not team_df.empty else "-"
    rej = (rejection_reason or "").replace("'", "''")
    try:
        session.sql(f"""INSERT INTO KIPI_RM_DB.RM_SCHEMA.ALLOCATION_HISTORY (ACTION_TYPE, ACTION_BY, PROJECT_NAME, CLIENT_NAME, SKILLS_REQUESTED, SE_PRIORITY_SKILLS, TEAM_SIZE, POSITIONS_REQUESTED, AVG_MATCH_SCORE, ON_BENCH_COUNT, ARCHITECTS_COUNT, SR_CONSULTANTS_COUNT, JR_CONSULTANTS_COUNT, PM_COUNT, TEAM_MEMBERS, REJECTION_REASON) VALUES ('{action_type}', CURRENT_USER(), '{proj}', '{client}', '{skills_str.replace("'", "''")}', '-', {team_size}, {positions}, {avg_score}, {bench_count}, {arch_c}, {sr_c}, {jr_c}, {pm_c}, '{members.replace("'", "''")}', '{rej}')""").collect()
    except Exception: pass

def reset_all():
    for k in ["team_result", "all_candidates", "approval_status", "valid_skills"]: st.session_state[k] = None
    st.session_state.plan_analyzed = False; st.session_state.skill_prof_df = pd.DataFrame(columns=["Skill", "Required Proficiency"]); st.session_state.uploader_key += 1; st.session_state.rejected_employees = set(); st.session_state.db_updates = []; st.session_state.approved_project = ""; st.session_state.approved_client = ""; st.session_state.feedback_summaries = {}

tab_matcher, tab_dashboard = st.tabs(["&#9878; Resource Matcher", "&#128202; Allocation Dashboard"])

with tab_dashboard:
    st.markdown("<div class='team-header'><h2>Allocation Dashboard</h2><p>Complete history of team allocation decisions</p></div>", unsafe_allow_html=True)
    hist_df = session.sql("SELECT * FROM KIPI_RM_DB.RM_SCHEMA.ALLOCATION_HISTORY ORDER BY ACTION_DATE DESC").to_pandas()
    if hist_df.empty:
        st.markdown("<div style='text-align:center; padding:4rem 2rem; color:#5a6784;'><div style='font-size:3.5rem; margin-bottom:0.8rem;'>&#128203;</div><h3 style='color:#1a2744;'>No Allocation History Yet</h3><p>Approve or reject a team in the Resource Matcher tab to see data here.</p></div>", unsafe_allow_html=True)
    else:
        total = len(hist_df); approved = len(hist_df[hist_df["ACTION_TYPE"] == "Approved"]); rejected = len(hist_df[hist_df["ACTION_TYPE"] == "Rejected"])
        approval_rate = round(approved / total * 100, 1) if total > 0 else 0; total_allocated = int(hist_df[hist_df["ACTION_TYPE"] == "Approved"]["TEAM_SIZE"].sum()); avg_score_all = round(hist_df[hist_df["ACTION_TYPE"] == "Approved"]["AVG_MATCH_SCORE"].mean(), 1) if approved > 0 else 0
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1: st.markdown(f"<div class='dash-card dash-total'><h1 style='color:#2d4a7a;'>{total}</h1><p>Total Actions</p></div>", unsafe_allow_html=True)
        with k2: st.markdown(f"<div class='dash-card dash-approved'><h1 style='color:#1a8754;'>{approved}</h1><p>Approved</p></div>", unsafe_allow_html=True)
        with k3: st.markdown(f"<div class='dash-card dash-rejected'><h1 style='color:#c62828;'>{rejected}</h1><p>Rejected</p></div>", unsafe_allow_html=True)
        with k4: st.markdown(f"<div class='dash-card dash-rate'><h1 style='color:#e65100;'>{approval_rate}%</h1><p>Approval Rate</p></div>", unsafe_allow_html=True)
        with k5: st.markdown(f"<div class='dash-card dash-approved'><h1 style='color:#1a8754;'>{total_allocated}</h1><p>People Allocated</p></div>", unsafe_allow_html=True)
        with k6: st.markdown(f"<div class='dash-card dash-total'><h1 style='color:#2d4a7a;'>{avg_score_all}</h1><p>Avg Match Score</p></div>", unsafe_allow_html=True)
        st.markdown("---"); dc1, dc2 = st.columns(2)
        with dc1:
            st.markdown("##### Approvals vs Rejections"); action_counts = hist_df["ACTION_TYPE"].value_counts().reset_index(); action_counts.columns = ["Action", "Count"]; st.bar_chart(action_counts, x="Action", y="Count")
        with dc2:
            st.markdown("##### Team Composition (Approved)"); approved_df = hist_df[hist_df["ACTION_TYPE"] == "Approved"]
            if not approved_df.empty: st.bar_chart(pd.DataFrame({"Role": ["Architects", "Sr. Consultants", "Jr. Consultants", "PMs"], "Count": [int(approved_df["ARCHITECTS_COUNT"].sum()), int(approved_df["SR_CONSULTANTS_COUNT"].sum()), int(approved_df["JR_CONSULTANTS_COUNT"].sum()), int(approved_df["PM_COUNT"].sum())]}), x="Role", y="Count")
            else: st.info("No approved allocations yet.")
        st.markdown("---"); st.markdown("##### Top Requested Skills")
        all_skills_hist = []
        for _, row in hist_df.iterrows():
            if row["SKILLS_REQUESTED"] and row["SKILLS_REQUESTED"] != "-":
                for sk in row["SKILLS_REQUESTED"].split(", "): all_skills_hist.append(sk.strip())
        if all_skills_hist: skill_freq = pd.DataFrame(all_skills_hist, columns=["Skill"]).value_counts().reset_index(); skill_freq.columns = ["Skill", "Requests"]; st.bar_chart(skill_freq.head(15), x="Skill", y="Requests")
        st.markdown("---"); st.markdown("##### Action Timeline")
        for _, row in hist_df.iterrows():
            action = row["ACTION_TYPE"]; css_class = "timeline-approved" if action == "Approved" else "timeline-rejected"; icon = "&#9989;" if action == "Approved" else "&#10060;"; date_str = str(row["ACTION_DATE"])[:19] if row["ACTION_DATE"] else ""
            with st.expander(f"{icon}  {action}  |  {row.get('PROJECT_NAME', '') or ''}  |  {row.get('CLIENT_NAME', '') or ''}  |  {date_str}", expanded=False):
                st.markdown(f"<div class='{css_class}'><b>Action:</b> {action} &bull; <b>Date:</b> {date_str} &bull; <b>By:</b> {row.get('ACTION_BY', '') or ''}<br><b>Project:</b> {row.get('PROJECT_NAME', '') or ''} &bull; <b>Client:</b> {row.get('CLIENT_NAME', '') or ''}<br><b>Team:</b> {int(row.get('TEAM_SIZE', 0))}/{int(row.get('POSITIONS_REQUESTED', 0))} filled &bull; <b>Avg Score:</b> {row.get('AVG_MATCH_SCORE', 0)}/100 &bull; <b>On Bench:</b> {int(row.get('ON_BENCH_COUNT', 0))}</div>", unsafe_allow_html=True)
                st.markdown(f"**Skills:** {row.get('SKILLS_REQUESTED', '-') or '-'}"); st.markdown(f"**Team Members:** {row.get('TEAM_MEMBERS', '-') or '-'}")
                if action == "Rejected" and row.get("REJECTION_REASON"): st.markdown(f"**Reason:** {row['REJECTION_REASON']}")
        st.markdown("---"); st.markdown("##### Full History Table")
        st.dataframe(hist_df[["ACTION_DATE", "ACTION_TYPE", "ACTION_BY", "PROJECT_NAME", "CLIENT_NAME", "TEAM_SIZE", "POSITIONS_REQUESTED", "AVG_MATCH_SCORE", "ON_BENCH_COUNT", "ARCHITECTS_COUNT", "SR_CONSULTANTS_COUNT", "JR_CONSULTANTS_COUNT", "PM_COUNT"]].rename(columns={"ACTION_DATE": "Date", "ACTION_TYPE": "Action", "ACTION_BY": "By", "PROJECT_NAME": "Project", "CLIENT_NAME": "Client", "TEAM_SIZE": "Filled", "POSITIONS_REQUESTED": "Requested", "AVG_MATCH_SCORE": "Avg Score", "ON_BENCH_COUNT": "Bench", "ARCHITECTS_COUNT": "Arch", "SR_CONSULTANTS_COUNT": "Sr.", "JR_CONSULTANTS_COUNT": "Jr.", "PM_COUNT": "PM"}), use_container_width=True, hide_index=True, height=350)

with tab_matcher:
    with st.sidebar:
        st.markdown("### Team Structure"); num_architects = st.selectbox("Architects (ASA & above)", QTY_OPTIONS, index=1); num_sr_consultants = st.selectbox("Sr. Consultants (Below ASA)", QTY_OPTIONS, index=1); num_jr_consultants = st.selectbox("Jr. Consultants (SE / Sr. SE)", QTY_OPTIONS, index=1); num_pm = st.selectbox("Project Managers", QTY_OPTIONS, index=0)
        total_team = num_architects + num_sr_consultants + num_jr_consultants + num_pm
        st.markdown(f"<div class='ts-total-bar'><span>Total Team</span><span>{total_team}</span></div>", unsafe_allow_html=True); st.divider(); st.markdown("### Scoring Model")
        st.markdown("| Factor | Weight |\n|:---|---:|\n| Skill + Proficiency | **40%** |\n| Performance | **25%** |\n| Technical | **15%** |\n| Certifications | **10%** |\n| Bench Status | **10%** |")

    st.markdown("<span class='step-badge'>STEP 1</span>", unsafe_allow_html=True); st.markdown("#### Upload 5A Plan Document")
    uploaded_file = st.file_uploader("Drop your 5A plan here", type=["txt", "csv"], label_visibility="collapsed", help="Accepted: TXT, CSV", key=f"file_uploader_{st.session_state.uploader_key}")

    if uploaded_file:
        plan_text = extract_file_text(uploaded_file)
        if plan_text:
            with st.expander("Preview uploaded document", expanded=False): st.code(plan_text[:3000] + ("..." if len(plan_text) > 3000 else ""), language=None)
            analyze_btn = st.button("Analyze Plan with AI", type="secondary", use_container_width=True)
            if analyze_btn:
                st.session_state.team_result = None; st.session_state.all_candidates = None; st.session_state.approval_status = None; st.session_state.rejected_employees = set(); st.session_state.feedback_summaries = {}
                progress = st.progress(0, text="Scanning document for skill keywords..."); text_matched = extract_skills_from_text(plan_text, available_skills)
                text_with_prof = [{"skill": s, "proficiency": detect_proficiency_hint(plan_text, s)} for s in text_matched]
                progress.progress(35, text="Running AI extraction with proficiency analysis..."); remaining_skills = [s for s in available_skills if s not in text_matched]; remaining_csv = ", ".join(remaining_skills)
                prompt = ("You are a strict skill extractor. A 5A account plan is provided below. Identify ONLY skills that the plan EXPLICITLY requires or names, along with the proficiency level the plan implies.\n\nSTRICT RULES:\n- ONLY return skills directly mentioned or unmistakably required by the plan\n- Do NOT infer supporting skills not mentioned in the plan\n- For each skill, determine proficiency from context clues:\n  Expert = deep expertise, mastery, extensive experience needed\n  Advanced = solid hands-on, proficient, experienced\n  Intermediate = working knowledge, competent (default if unclear)\n  Beginner = basic awareness, foundational\n- Return EMPTY array [] if no skills match\n\n" f"CANDIDATE SKILLS (choose ONLY from this list): {remaining_csv}\n\nPLAN TEXT:\n{plan_text[:4000]}\n\n" 'Return ONLY a valid JSON array of objects: [{"skill":"X","proficiency":"Advanced"}, ...]\nJSON:')
                ai_results = []
                try:
                    raw_response = session.create_dataframe([[prompt]], schema=["PROMPT"]).select(call_function("SNOWFLAKE.CORTEX.COMPLETE", lit("llama3.1-70b"), col("PROMPT")).alias("RESPONSE")).collect()[0]["RESPONSE"]
                    match = re.search(r"\[.*?\]", raw_response, re.DOTALL); parsed = json.loads(match.group()) if match else json.loads(raw_response.strip())
                    for item in parsed:
                        if isinstance(item, dict):
                            sk = item.get("skill", ""); pr = item.get("proficiency", "Intermediate")
                            if sk in remaining_skills and pr in PROFICIENCY_LEVELS: ai_results.append({"skill": sk, "proficiency": pr})
                        elif isinstance(item, str) and item in remaining_skills: ai_results.append({"skill": item, "proficiency": "Intermediate"})
                except Exception: ai_results = []
                progress.progress(75, text="Adding companion skills..."); all_skills_extracted = text_with_prof + ai_results; existing_names = {s["skill"] for s in all_skills_extracted}; companions_added = []
                for s in all_skills_extracted:
                    for comp_skill, comp_prof in SKILL_COMPANIONS.get(s["skill"], []):
                        if comp_skill not in existing_names and comp_skill in available_skills: companions_added.append({"skill": comp_skill, "proficiency": comp_prof}); existing_names.add(comp_skill)
                if companions_added: all_skills_extracted.extend(companions_added)
                progress.progress(100, text="Analysis complete."); st.session_state.companion_names = {c["skill"] for c in companions_added}
                st.session_state.skill_prof_df = pd.DataFrame([{"Skill": s["skill"], "Required Proficiency": s["proficiency"]} for s in all_skills_extracted]) if all_skills_extracted else pd.DataFrame(columns=["Skill", "Required Proficiency"])
                st.session_state.plan_analyzed = True

            if st.session_state.plan_analyzed:
                if not st.session_state.skill_prof_df.empty:
                    st.markdown("**Detected skills with proficiency:**"); render_skill_pills_with_prof(st.session_state.skill_prof_df, st.session_state.get("companion_names", set()))
                    comp_names = st.session_state.get("companion_names", set())
                    if comp_names: st.caption(f"Companion skills auto-added: {', '.join(sorted(comp_names))}")
                if st.session_state.rejected_employees: st.markdown(f"<div class='rejected-banner'>Previously rejected: {len(st.session_state.rejected_employees)} candidates excluded from results</div>", unsafe_allow_html=True)
                valid_rows = st.session_state.skill_prof_df.dropna(subset=["Skill", "Required Proficiency"]).drop_duplicates(subset=["Skill"])
                if valid_rows.empty: st.warning("No skills detected. Re-analyze the plan."); st.stop()
                if total_team == 0: st.warning("Set at least one role count in the sidebar."); st.stop()
                st.markdown("---"); st.markdown("<span class='step-badge'>STEP 2</span>", unsafe_allow_html=True); st.markdown("#### Build Recommended Team")
                submitted = st.button("Find Best Team", type="primary", use_container_width=True)

                if submitted:
                    st.session_state.approval_status = None; n = len(valid_rows); skill_conditions = []
                    for _, r in valid_rows.iterrows(): skill_conditions.append(f"('{r['Skill'].replace(chr(39), chr(39)+chr(39))}', {PROF_RANK[r['Required Proficiency']]})")
                    values_clause = ", ".join(skill_conditions); arch_list = "', '".join(ROLE_MAP["ARCHITECT"]); sr_list = "', '".join(ROLE_MAP["SR_CONSULTANT"]); jr_list = "', '".join(ROLE_MAP["JR_CONSULTANT"]); pm_list = "', '".join(ROLE_MAP["PM"])
                    query = f"""WITH required_skills(skill_name, req_rank) AS (SELECT * FROM VALUES {values_clause}),
                    skill_match AS (SELECT sd.EMPLOYEE_ID, COUNT(DISTINCT sd.SKILL_NAME) AS matched_skills, SUM(CASE WHEN CASE sd.PROFICIENCY_LEVEL WHEN 'Expert' THEN 4 WHEN 'Advanced' THEN 3 WHEN 'Intermediate' THEN 2 WHEN 'Beginner' THEN 1 ELSE 0 END >= rs.req_rank THEN 1 ELSE 0 END) AS proficiency_met, ROUND(AVG(sd.SELF_RATING), 2) AS avg_skill_rating, ROUND(AVG(sd.YEARS_OF_EXPERIENCE), 1) AS avg_skill_exp, LISTAGG(DISTINCT sd.SKILL_NAME || ' (' || sd.PROFICIENCY_LEVEL || ')', ', ') WITHIN GROUP (ORDER BY sd.SKILL_NAME || ' (' || sd.PROFICIENCY_LEVEL || ')') AS matching_skills FROM KIPI_RM_DB.RM_SCHEMA.SKILLS_DETAILS sd JOIN required_skills rs ON sd.SKILL_NAME = rs.skill_name GROUP BY sd.EMPLOYEE_ID),
                    missing_skills AS (SELECT sm.EMPLOYEE_ID, LISTAGG(rs.skill_name, ', ') WITHIN GROUP (ORDER BY rs.skill_name) AS missing_skill_names, COUNT(rs.skill_name) AS missing_skill_count FROM skill_match sm CROSS JOIN required_skills rs WHERE NOT EXISTS (SELECT 1 FROM KIPI_RM_DB.RM_SCHEMA.SKILLS_DETAILS sd WHERE sd.EMPLOYEE_ID = sm.EMPLOYEE_ID AND sd.SKILL_NAME = rs.skill_name) GROUP BY sm.EMPLOYEE_ID),
                    feedback_agg AS (SELECT EMPLOYEE_ID, ROUND(AVG(OVERALL_RATING), 2) AS avg_overall, ROUND(AVG(TECHNICAL_RATING), 2) AS avg_technical, ROUND(AVG(DELIVERY_RATING), 2) AS avg_delivery, ROUND(AVG(COMMUNICATION_RATING), 2) AS avg_communication, ROUND(AVG(TEAMWORK_RATING), 2) AS avg_teamwork, COUNT(*) AS review_count FROM KIPI_RM_DB.RM_SCHEMA.FEEDBACK_DETAILS GROUP BY EMPLOYEE_ID),
                    cert_agg AS (SELECT EMPLOYEE_ID, COUNT(*) AS active_certs, LISTAGG(DISTINCT CERTIFICATION_NAME, ', ') WITHIN GROUP (ORDER BY CERTIFICATION_NAME) AS cert_names FROM KIPI_RM_DB.RM_SCHEMA.CERTIFICATION_DETAILS WHERE CERTIFICATION_STATUS = 'Active' GROUP BY EMPLOYEE_ID),
                    bench AS (SELECT EMPLOYEE_ID, AVAILABILITY_STATUS, PREFERRED_ROLE, BENCH_START_DATE FROM KIPI_RM_DB.RM_SCHEMA.BENCH_DETAILS WHERE BENCH_END_DATE IS NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_ID ORDER BY BENCH_START_DATE DESC) = 1),
                    margin_agg AS (SELECT EMPLOYEE_ID, ROUND(AVG(BILLING_RATE_PER_HOUR), 2) AS avg_billing_rate, ROUND(AVG(COST_RATE_PER_HOUR), 2) AS avg_cost_rate, ROUND(AVG(MARGIN_AMOUNT_PER_HOUR), 2) AS avg_margin_per_hour, ROUND(AVG(MARGIN_PERCENTAGE), 2) AS avg_margin_pct, ROUND(SUM(MONTHLY_REVENUE), 2) AS total_revenue, ROUND(SUM(MONTHLY_COST), 2) AS total_cost, ROUND(SUM(MONTHLY_MARGIN), 2) AS total_margin FROM KIPI_RM_DB.RM_SCHEMA.MARGIN_DATA WHERE IS_ACTIVE = TRUE GROUP BY EMPLOYEE_ID),
                    scored AS (SELECT e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.DESIGNATION, e.DEPARTMENT, e.BUSINESS_UNIT, e.LOCATION, e.BILLABLE_STATUS, e.TOTAL_EXPERIENCE_YEARS, e.EMPLOYMENT_STATUS, e.EMPLOYEE_LWD, sm.matched_skills, sm.proficiency_met, sm.matching_skills, sm.avg_skill_rating, sm.avg_skill_exp, COALESCE(f.avg_overall, 0) AS avg_overall, COALESCE(f.avg_technical, 0) AS avg_technical, COALESCE(f.avg_delivery, 0) AS avg_delivery, COALESCE(f.avg_communication, 0) AS avg_communication, COALESCE(f.avg_teamwork, 0) AS avg_teamwork, COALESCE(f.review_count, 0) AS review_count, COALESCE(c.active_certs, 0) AS active_certs, COALESCE(c.cert_names, '-') AS certifications, CASE WHEN b.EMPLOYEE_ID IS NOT NULL THEN 1 ELSE 0 END AS on_bench, COALESCE(b.AVAILABILITY_STATUS, 'Not on Bench') AS availability, CASE WHEN b.EMPLOYEE_ID IS NOT NULL THEN DATEDIFF('day', b.BENCH_START_DATE, CURRENT_DATE()) ELSE 0 END AS bench_days, COALESCE(mg.avg_billing_rate, 0) AS avg_billing_rate, COALESCE(mg.avg_cost_rate, 0) AS avg_cost_rate, COALESCE(mg.avg_margin_per_hour, 0) AS avg_margin_per_hour, COALESCE(mg.avg_margin_pct, 0) AS avg_margin_pct, COALESCE(mg.total_revenue, 0) AS total_revenue, COALESCE(mg.total_cost, 0) AS total_cost, COALESCE(mg.total_margin, 0) AS total_margin, COALESCE(ms.missing_skill_names, '-') AS missing_skills, COALESCE(ms.missing_skill_count, 0) AS missing_skill_count, CASE WHEN b.EMPLOYEE_ID IS NOT NULL THEN 1 WHEN e.BILLABLE_STATUS IN ('Non-Billable', 'Partially Billable') THEN 2 ELSE 3 END AS availability_priority, ROUND((sm.proficiency_met::FLOAT / {n} * 40) + (COALESCE(f.avg_overall, 0) / 5.0 * 25) + (COALESCE(f.avg_technical, 0) / 5.0 * 15) + (LEAST(COALESCE(c.active_certs, 0), 5) / 5.0 * 10) + (CASE WHEN b.EMPLOYEE_ID IS NOT NULL THEN 10 ELSE 0 END), 1) AS match_score, CASE WHEN e.DESIGNATION IN ('{arch_list}') THEN 'ARCHITECT' WHEN e.DESIGNATION IN ('{sr_list}') THEN 'SR_CONSULTANT' WHEN e.DESIGNATION IN ('{jr_list}') THEN 'JR_CONSULTANT' WHEN e.DESIGNATION IN ('{pm_list}') THEN 'PM' ELSE 'OTHER' END AS role_category FROM skill_match sm JOIN KIPI_RM_DB.RM_SCHEMA.EMPLOYEES e ON sm.EMPLOYEE_ID = e.EMPLOYEE_ID LEFT JOIN feedback_agg f ON e.EMPLOYEE_ID = f.EMPLOYEE_ID LEFT JOIN cert_agg c ON e.EMPLOYEE_ID = c.EMPLOYEE_ID LEFT JOIN bench b ON e.EMPLOYEE_ID = b.EMPLOYEE_ID LEFT JOIN margin_agg mg ON e.EMPLOYEE_ID = mg.EMPLOYEE_ID LEFT JOIN missing_skills ms ON e.EMPLOYEE_ID = ms.EMPLOYEE_ID WHERE e.EMPLOYMENT_STATUS in ( 'Active','On Notice'))
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY role_category ORDER BY availability_priority ASC, match_score DESC) AS role_rank FROM scored ORDER BY CASE role_category WHEN 'ARCHITECT' THEN 1 WHEN 'SR_CONSULTANT' THEN 2 WHEN 'JR_CONSULTANT' THEN 3 WHEN 'PM' THEN 4 ELSE 5 END, availability_priority ASC, match_score DESC"""
                    with st.spinner("Scoring and ranking candidates..."): df = session.sql(query).to_pandas()
                    if df.empty: st.warning("No matching resources found for the selected skills."); st.stop()
                    if st.session_state.rejected_employees:
                        df = df[~df["EMPLOYEE_ID"].isin(st.session_state.rejected_employees)]
                        if df.empty: st.warning("All matching candidates have been rejected."); st.stop()
                    OPTIONS_PER_SLOT = 3; picks = {}
                    if num_architects > 0: picks["ARCHITECT"] = df[df["ROLE_CATEGORY"] == "ARCHITECT"].head(num_architects * OPTIONS_PER_SLOT)
                    if num_sr_consultants > 0: picks["SR_CONSULTANT"] = df[df["ROLE_CATEGORY"] == "SR_CONSULTANT"].head(num_sr_consultants * OPTIONS_PER_SLOT)
                    if num_jr_consultants > 0: picks["JR_CONSULTANT"] = df[df["ROLE_CATEGORY"] == "JR_CONSULTANT"].head(num_jr_consultants * OPTIONS_PER_SLOT)
                    if num_pm > 0: picks["PM"] = df[df["ROLE_CATEGORY"] == "PM"].head(num_pm * OPTIONS_PER_SLOT)
                    team = pd.concat([v for v in picks.values() if not v.empty])
                    if team.empty: st.warning("Could not form a team with matching candidates."); st.stop()
                    st.session_state.team_result = team; st.session_state.all_candidates = df; st.session_state.valid_skills = valid_rows.copy()
                    st.session_state.feedback_summaries = get_feedback_summaries(team["EMPLOYEE_ID"].tolist())

            if st.session_state.team_result is not None and st.session_state.approval_status is None:
                team = st.session_state.team_result; df = st.session_state.all_candidates; n = len(st.session_state.valid_skills)
                st.markdown(f"<div class='team-header'><h2>Recommended Team</h2><p>{n} required skills &bull; {total_team} positions &bull; 3 options each</p></div>", unsafe_allow_html=True)
                m1, m2, m3 = st.columns(3); m1.metric("Positions", f"{total_team}"); m2.metric("Options Shown", f"{len(team)}"); m3.metric("On Bench", int(team["ON_BENCH"].sum()))

                def render_profile(row, role_label, n_skills, feedback=None):
                    is_pm = row.get("ROLE_CATEGORY") == "PM"; prof_met = int(row["PROFICIENCY_MET"])
                    with st.expander(f"{role_label}  |  {row['EMPLOYEE_NAME']}  |  {row['DESIGNATION']}  |  Score: {row['MATCH_SCORE']}/100  |  Prof Met: {prof_met}/{n_skills}", expanded=False):
                        p1, p2, p3, p4, p5, p6 = st.columns(6); p1.metric("Match Score", f"{row['MATCH_SCORE']}"); p2.metric("Prof. Met", f"{prof_met} / {n_skills}"); p3.metric("Overall", f"{row['AVG_OVERALL']} / 5"); p4.metric("Technical", f"{row['AVG_TECHNICAL']} / 5"); p5.metric("Skills #", f"{int(row['MATCHED_SKILLS'])} / {n_skills}"); p6.metric("Exp.", f"{row['TOTAL_EXPERIENCE_YEARS']} yrs")
                        st.markdown(f"<div class='profile-section'><b>Designation:</b> {row['DESIGNATION']} &bull; <b>Dept:</b> {row['DEPARTMENT']} &bull; <b>BU:</b> {row['BUSINESS_UNIT']} &bull; <b>Location:</b> {row['LOCATION']} &bull; <b>Billable:</b> {row['BILLABLE_STATUS']}</div>", unsafe_allow_html=True)
                        if not is_pm:
                            st.markdown(f"**Matched Skills ({int(row['MATCHED_SKILLS'])}/{n_skills}):** " + "".join([f"<span class='skill-pill'>{s.strip()}</span>" for s in row["MATCHING_SKILLS"].split(",")]), unsafe_allow_html=True)
                            missing = row.get("MISSING_SKILLS", "-")
                            if missing and missing != "-": st.markdown(f"**Missing Skills ({int(row.get('MISSING_SKILL_COUNT', 0))}/{n_skills}):** " + "".join([f"<span class='missing-skill-pill'>{s.strip()}</span>" for s in missing.split(",")]), unsafe_allow_html=True)
                            else: st.markdown("**Missing Skills:** None — full skill coverage &#9989;", unsafe_allow_html=True)
                        st.markdown(f"**Certifications:** {row['CERTIFICATIONS']}"); st.markdown("<div class='margin-section'><b>Cost & Margin</b></div>", unsafe_allow_html=True)
                        cm1, cm2, cm3, cm4 = st.columns(4); cm1.metric("Billing Rate/Hr", f"${row.get('AVG_BILLING_RATE', 0):.2f}"); cm2.metric("Cost Rate/Hr", f"${row.get('AVG_COST_RATE', 0):.2f}"); cm3.metric("Margin/Hr", f"${row.get('AVG_MARGIN_PER_HOUR', 0):.2f}"); cm4.metric("Margin %", f"{row.get('AVG_MARGIN_PCT', 0):.1f}%")
                        r1, r2, r3, r4 = st.columns(4); r1.metric("Communication", f"{row['AVG_COMMUNICATION']} / 5"); r2.metric("Teamwork", f"{row['AVG_TEAMWORK']} / 5"); r3.metric("Delivery", f"{row['AVG_DELIVERY']} / 5"); r4.metric("Reviews", f"{int(row['REVIEW_COUNT'])}")
                        if feedback and isinstance(feedback, dict):
                            fb_summary = feedback.get("summary", "")
                            fb_neg = feedback.get("negatives", "")
                            if fb_summary:
                                neg_html = ""
                                if fb_neg:
                                    neg_keywords = [nk.strip() for nk in fb_neg.split("|") if nk.strip()]
                                    neg_pills = "".join([f"<span style='background:#ffeaea; color:#c62828; padding:2px 8px; border-radius:10px; font-size:0.78em; margin:2px 3px; display:inline-block; border:1px solid #ef9a9a;'>{kw}</span>" for kw in neg_keywords])
                                    neg_html = f"<div style='margin-top:6px;'><b style='color:#c62828;'>&#9888; Negative Sentiment:</b> {neg_pills}</div>"
                                st.markdown(f"<div style='background:#f0f4ff; border-left:3px solid #4a7cff; padding:10px 14px; margin:8px 0; border-radius:4px; font-size:0.88em;'><b>&#128172; Feedback Summary:</b> {fb_summary}{neg_html}</div>", unsafe_allow_html=True)                            

                section_config = [("ARCHITECT", num_architects, "Architects", "ASA & above"), ("SR_CONSULTANT", num_sr_consultants, "Senior Consultants", "Lead Engineer / Sr. Lead Engineer"), ("JR_CONSULTANT", num_jr_consultants, "Junior Consultants", "SE / Sr. SE"), ("PM", num_pm, "Project Managers", "PM & Sr. PM")]

                st.markdown("---")
                selected_emp_ids = []
                for cat_key, cat_count, cat_title, cat_desc in section_config:
                    if cat_count == 0: continue
                    st.markdown(f"##### {cat_title} ({cat_count} positions &mdash; select 1 of 3 per position)")
                    st.caption(cat_desc)
                    cat_df = team[team["ROLE_CATEGORY"] == cat_key]
                    if not cat_df.empty:
                        candidates = list(cat_df.iterrows())
                        for slot in range(cat_count):
                            slot_candidates = candidates[slot * 3 : (slot + 1) * 3]
                            if not slot_candidates:
                                st.warning(f"No candidates for {cat_title} position #{slot + 1}.")
                                continue
                            st.markdown(f"###### Position {slot + 1}")
                            radio_options = []
                            radio_map = {}
                            for opt_num, (_, row) in enumerate(slot_candidates, 1):
                                bench_tag = f" | Bench: {int(row.get('BENCH_DAYS', 0))}d" if row["ON_BENCH"] == 1 else ""
                                dot = "🟢" if row["ON_BENCH"] == 1 else ("🟠" if row.get("BILLABLE_STATUS") == "Partially Billable" else ("🔴" if row.get("BILLABLE_STATUS") == "Billable" else "⚪"))
                                fb_check = st.session_state.get("feedback_summaries", {}).get(int(row["EMPLOYEE_ID"]), {})
                                neg_flag = " | ⚠️ Review Feedback" if isinstance(fb_check, dict) and fb_check.get("negatives", "") else ""
                                lwd_tag = f" | LWD: {row['EMPLOYEE_LWD']}" if row.get("EMPLOYMENT_STATUS") == "On Notice" and row.get("EMPLOYEE_LWD") else ""
                                lbl = f"{dot} {row['EMPLOYEE_NAME']} | {row['DESIGNATION']} | Score: {row['MATCH_SCORE']}/100{bench_tag}{neg_flag}{lwd_tag}"
                                radio_options.append(lbl)
                                radio_map[lbl] = int(row["EMPLOYEE_ID"])
                            selected = st.radio(f"Select for {cat_title} #{slot + 1}", radio_options, key=f"sel_{cat_key}_{slot}")
                            if selected:
                                selected_emp_ids.append(radio_map[selected])
                            for opt_num, (_, row) in enumerate(slot_candidates, 1):
                                is_sel = (selected and radio_map.get(selected) == int(row["EMPLOYEE_ID"]))
                                bench_tag = f"  |  Bench: {int(row.get('BENCH_DAYS', 0))}d" if row["ON_BENCH"] == 1 else ""
                                avail_dot = "&#128994;" if row["ON_BENCH"] == 1 else ("&#128992;" if row.get("BILLABLE_STATUS") == "Partially Billable" else ("&#128308;" if row.get("BILLABLE_STATUS") == "Billable" else "&#9898;"))
                                pick_tag = " &#9989; SELECTED" if is_sel else ""
                                lwd_tag = f" | LWD: {row['EMPLOYEE_LWD']}" if row.get("EMPLOYMENT_STATUS") == "On Notice" and row.get("EMPLOYEE_LWD") else ""                                                                
                                emp_fb = st.session_state.get("feedback_summaries", {}).get(int(row["EMPLOYEE_ID"]), {})
                                render_profile(row, f"{avail_dot} Option {opt_num}{bench_tag}{pick_tag}{lwd_tag}", n, feedback=emp_fb)
                        if len(candidates) < cat_count * 3:
                            st.warning(f"Only {len(candidates)} candidates found for {cat_count} {cat_title.lower()} positions.")
                    else:
                        st.warning(f"No {cat_title.lower()} found matching the required skills.")

                selected_team = team[team["EMPLOYEE_ID"].isin(selected_emp_ids)]
                if not selected_team.empty:
                    st.markdown("---")
                    st.markdown(f"<div class='selected-summary'><b>&#9989; Selected Team ({len(selected_team)} of {total_team})</b> &mdash; Only these candidates will be allocated on approval:</div>", unsafe_allow_html=True)
                    for _, row in selected_team.iterrows():
                        bench_tag = f" | Bench: {int(row.get('BENCH_DAYS', 0))}d" if row["ON_BENCH"] == 1 else ""
                        sel_fb = st.session_state.get("feedback_summaries", {}).get(int(row["EMPLOYEE_ID"]), {})
                        sel_summary = sel_fb.get("summary", "") if isinstance(sel_fb, dict) else str(sel_fb)
                        sel_neg = sel_fb.get("negatives", "") if isinstance(sel_fb, dict) else ""
                        summary_text = f" &mdash; *{sel_summary}*" if sel_summary else ""
                        if sel_neg:
                            summary_text += f" &mdash; <span style='color:#c62828;'>&#9888; {sel_neg}</span>"
                        lwd_tag = f" | <b>LWD: {row['EMPLOYEE_LWD']}</b>" if row.get("EMPLOYMENT_STATUS") == "On Notice" and row.get("EMPLOYEE_LWD") else ""
                        st.markdown(f"- **{row['EMPLOYEE_NAME']}** &mdash; {row['DESIGNATION']} &mdash; {row['ROLE_CATEGORY']} &mdash; Score: {row['MATCH_SCORE']}/100{bench_tag}{lwd_tag}{summary_text}", unsafe_allow_html=True)                        

                st.markdown("---")
                st.markdown("##### All Scored Candidates")
                display_df = df[["EMPLOYEE_NAME", "DESIGNATION", "ROLE_CATEGORY", "DEPARTMENT", "LOCATION", "MATCHED_SKILLS", "MISSING_SKILL_COUNT", "PROFICIENCY_MET", "MATCHING_SKILLS", "MISSING_SKILLS", "AVG_OVERALL", "AVG_TECHNICAL", "ACTIVE_CERTS", "ON_BENCH", "AVG_BILLING_RATE", "AVG_COST_RATE", "AVG_MARGIN_PCT", "TOTAL_MARGIN", "MATCH_SCORE"]].rename(columns={"EMPLOYEE_NAME": "Name", "DESIGNATION": "Role", "ROLE_CATEGORY": "Category", "DEPARTMENT": "Dept", "LOCATION": "Location", "MATCHED_SKILLS": "Matched #", "MISSING_SKILL_COUNT": "Missing #", "PROFICIENCY_MET": "Prof Met", "MATCHING_SKILLS": "Matched Skills", "MISSING_SKILLS": "Missing Skills", "AVG_OVERALL": "Rating", "AVG_TECHNICAL": "Tech", "ACTIVE_CERTS": "Certs", "ON_BENCH": "Bench", "AVG_BILLING_RATE": "Bill Rate", "AVG_COST_RATE": "Cost Rate", "AVG_MARGIN_PCT": "Margin %", "TOTAL_MARGIN": "Total Margin", "MATCH_SCORE": "Score"})
                st.dataframe(display_df.style.background_gradient(subset=["Score"], cmap="Greens"), use_container_width=True, hide_index=True, height=450)

                st.markdown("---")
                st.markdown("<div class='approval-box'><h3>RM Approval</h3><p>Review the selected team above and approve or reject.</p></div>", unsafe_allow_html=True)
                ac1, ac2 = st.columns(2)
                with ac1: project_name = st.text_input("Project / Engagement Name", placeholder="e.g. Data Platform Modernization")
                with ac2: client_name = st.text_input("Client Name", placeholder="e.g. Acme Corp")
                col_accept, col_reject = st.columns(2)
                with col_accept: accept_btn = st.button("Approve & Send Email", type="primary", use_container_width=True)
                with col_reject: reject_btn = st.button("Reject & Revise Skills", type="secondary", use_container_width=True)

                if accept_btn:
                    if not project_name.strip() or not client_name.strip(): st.error("Please enter both Project Name and Client Name before approving."); st.stop()
                    if selected_team.empty: st.error("No candidates selected. Please select one candidate per position using the radio buttons above."); st.stop()
                    with st.spinner("Approving allocation and updating records..."):
                        email_body = build_email_body(selected_team, st.session_state.valid_skills, section_config)
                        emp_ids = selected_team["EMPLOYEE_ID"].tolist(); emp_ids_sql = ", ".join([str(int(eid)) for eid in emp_ids])
                        proj = project_name.strip().replace("'", "''"); client = client_name.strip().replace("'", "''"); updates_done = []
                        try:
                            bench_result = session.sql(f"""UPDATE KIPI_RM_DB.RM_SCHEMA.BENCH_DETAILS SET BENCH_END_DATE = CURRENT_DATE() WHERE EMPLOYEE_ID IN ({emp_ids_sql}) AND BENCH_END_DATE IS NULL""").collect()
                            updates_done.append(f"Bench closed: {bench_result[0][0] if bench_result else 0} entries")
                        except Exception: updates_done.append("Bench update: skipped (error)")
                        try:
                            billable_result = session.sql(f"""UPDATE KIPI_RM_DB.RM_SCHEMA.EMPLOYEES SET BILLABLE_STATUS = 'Billable' WHERE EMPLOYEE_ID IN ({emp_ids_sql}) AND BILLABLE_STATUS != 'Billable'""").collect()
                            updates_done.append(f"Billable status updated: {billable_result[0][0] if billable_result else 0} employees")
                        except Exception: updates_done.append("Billable update: skipped (error)")
                        try:
                            margin_values = []
                            for _, row in selected_team.iterrows():
                                eid = int(row["EMPLOYEE_ID"]); rcat = row.get("ROLE_CATEGORY", "")
                                rate = 160.00 if "ARCHITECT" in rcat else (140.00 if "PM" in rcat else 120.00)
                                cost = round(rate * 0.38, 2); margin = round(rate - cost, 2); margin_pct = round(margin / rate * 100, 2)
                                margin_values.append(f"({eid}, '{proj}', '{client}', CURRENT_DATE(), NULL, {rate}, {cost}, {margin}, {margin_pct}, 160.0, {round(rate * 160, 2)}, {round(cost * 160, 2)}, {round(margin * 160, 2)}, 'Time & Material', 'USD', 'Consulting', TRUE)")
                            if margin_values: session.sql(f"""INSERT INTO KIPI_RM_DB.RM_SCHEMA.MARGIN_DATA (EMPLOYEE_ID, PROJECT_NAME, CLIENT_NAME, BILLING_START_DATE, BILLING_END_DATE, BILLING_RATE_PER_HOUR, COST_RATE_PER_HOUR, MARGIN_AMOUNT_PER_HOUR, MARGIN_PERCENTAGE, MONTHLY_BILLED_HOURS, MONTHLY_REVENUE, MONTHLY_COST, MONTHLY_MARGIN, BILLING_TYPE, CURRENCY, ENGAGEMENT_TYPE, IS_ACTIVE) VALUES {', '.join(margin_values)}""").collect(); updates_done.append(f"Margin records created: {len(margin_values)} entries")
                        except Exception as e: updates_done.append(f"Margin insert: skipped ({e})")
                        try:
                            alloc_values = []
                            for _, row in selected_team.iterrows():
                                eid = int(row["EMPLOYEE_ID"]); ename = row["EMPLOYEE_NAME"].replace("'", "''"); desig = row["DESIGNATION"].replace("'", "''"); rcat = row.get("ROLE_CATEGORY", "OTHER")
                                rate = 160.00 if "ARCHITECT" in rcat else (140.00 if "PM" in rcat else 120.00); c_rate = round(rate * 0.38, 2); m_pct = round((rate - c_rate) / rate * 100, 2)
                                alloc_values.append(f"({eid}, '{ename}', '{desig}', '{rcat}', '{proj}', '{client}', {row['MATCH_SCORE']}, {int(row['PROFICIENCY_MET'])}, '{row['MATCHING_SKILLS'].replace(chr(39), chr(39)+chr(39))}', '{row.get('MISSING_SKILLS', '-').replace(chr(39), chr(39)+chr(39))}', {rate}, {c_rate}, {m_pct}, {row['ON_BENCH'] == 1}, CURRENT_USER(), 'Active')")
                            if alloc_values: session.sql(f"""INSERT INTO KIPI_RM_DB.RM_SCHEMA.RESOURCE_ALLOCATIONS (EMPLOYEE_ID, EMPLOYEE_NAME, DESIGNATION, ROLE_CATEGORY, PROJECT_NAME, CLIENT_NAME, MATCH_SCORE, PROFICIENCY_MET, SKILLS_MATCHED, MISSING_SKILLS, BILLING_RATE_PER_HOUR, COST_RATE_PER_HOUR, MARGIN_PERCENTAGE, WAS_ON_BENCH, ALLOCATED_BY, ALLOCATION_STATUS) VALUES {', '.join(alloc_values)}""").collect(); updates_done.append(f"Resource allocations recorded: {len(alloc_values)} entries")
                        except Exception as e: updates_done.append(f"Allocation tracking: skipped ({e})")
                        log_allocation_action("Approved", project_name.strip(), client_name.strip(), selected_team, st.session_state.valid_skills, section_config)
                        st.session_state.db_updates = updates_done
                        try: session.sql(f"""CALL SYSTEM$SEND_EMAIL('RM_EMAIL_INTEGRATION', 'suhas.m.pachpute@kipi.ai', 'Team Allocation Approved - {proj}', $${email_body}$$)""").collect()
                        except Exception: pass
                        st.session_state.approval_status = "approved"; st.session_state.approved_project = project_name.strip(); st.session_state.approved_client = client_name.strip(); st.rerun()

                if reject_btn:
                    log_allocation_action("Rejected", "", "", st.session_state.team_result, st.session_state.valid_skills, section_config, "Skills mismatch - revising")
                    if st.session_state.team_result is not None: st.session_state.rejected_employees = st.session_state.rejected_employees | set(st.session_state.team_result["EMPLOYEE_ID"].tolist())
                    st.session_state.team_result = None; st.session_state.all_candidates = None; st.session_state.valid_skills = None; st.session_state.approval_status = None; st.rerun()

            if st.session_state.approval_status == "approved":
                st.markdown(f"<div style='text-align:center; padding:3rem; background:linear-gradient(135deg,#e8f4e8,#d4edda); border-radius:12px; margin:2rem 0;'><div style='font-size:3rem; margin-bottom:0.5rem;'>&#9989;</div><h2 style='color:#1a5928; margin:0;'>Allocation Approved</h2><p style='color:#2d6a3e; margin-top:0.5rem;'>Project: {st.session_state.get('approved_project', '')} | Client: {st.session_state.get('approved_client', '')}</p></div>", unsafe_allow_html=True)
                if st.session_state.get("db_updates"):
                    with st.expander("Database Updates Applied", expanded=True):
                        for update in st.session_state.db_updates: st.markdown(f"- {update}")
                if st.button("Start New Allocation", use_container_width=True): reset_all(); st.rerun()

    else:
        st.markdown("<div style='text-align:center; padding:4rem 2rem; color:#5a6784;'><div style='font-size:3.5rem; margin-bottom:0.8rem;'>&#128196;</div><h3 style='color:#1a2744;'>Upload a 5A Plan to Get Started</h3><p>Supported formats: TXT, CSV</p></div>", unsafe_allow_html=True)
