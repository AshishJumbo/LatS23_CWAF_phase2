import pandas as pd
import scipy.stats as stats

# important we analyze with aggregated observation at
#  ----for both analysis we will do a intent to treat and treated analysis----
#  ---------------------------------------------------------------------------
#  ===with in problem next action===
#  TREATED ANALYSIS:
#  1. next action correctness
#  ---------------------------------


# Question:
#  There are multiple outcomes so is it MANOVA or MANCOVA.
#  =======================================================

df_feedback_info = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_feedback_info.csv")
df_assignment_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_assignment_logs.csv")

# dev-note:
#  we need to figure out if the learner worked on the asignment multiple times
df_assignment_logs_first_time = df_assignment_logs.groupby(
    ['student_xid', 'assignment_xid', 'assignment_log_id']).size().reset_index(name='frequency')
df_assignment_logs_first_time.sort_values(by=['student_xid', 'assignment_xid', 'assignment_log_id'], inplace=True)
# dev-note: drop every assignment_log_id but the first time the learner works on the assignment
df_assignment_logs_first_time.drop_duplicates(subset=['student_xid', 'assignment_xid'], keep='first', inplace=True)

df_assignment_logs = df_assignment_logs.loc[
    df_assignment_logs.assignment_log_id.isin(df_assignment_logs_first_time.assignment_log_id.unique())]
df_assignment_logs.reset_index(drop=True, inplace=True)
df_assignment_logs.sort_values(by=['student_xid', 'assignment_xid', 'assignment_log_id'], inplace=True)

# dev-note: now use the filtered assignment logs to clean up problem and action logs
df_problem_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_problem_logs.csv")
df_problem_logs = df_problem_logs.loc[
    df_problem_logs.assignment_log_id.isin(df_assignment_logs_first_time.assignment_log_id.unique())]
df_problem_logs.reset_index(drop=True, inplace=True)
df_action_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_action_logs.csv")
df_action_logs = df_action_logs.loc[
    df_action_logs.assignment_log_id.isin(df_assignment_logs_first_time.assignment_log_id.unique())]
df_action_logs.reset_index(drop=True, inplace=True)

