# dev-note:
#   this file processes the logs form the student service that can be used
#   to log the students randomized in a 90:10 proportion
#   so that we can pull the problem logs and action logs form teh core

import pandas as pd

df_ss_control_treatment_assignment = pd.read_csv(
    "../data/rq3_Data/raw_from_student_support/CWAF_randomization_support_service_90_10_split.csv")
df_ss_control_treatment_assignment["control_treatment_assignment"] = 'CWAF_treatment'
df_ss_control_treatment_assignment.loc[df_ss_control_treatment_assignment.ss_selected_feedback_ids == "{NULL}",
                                       'control_treatment_assignment'] = 'control'

pr_ctrl_treat_grps = df_ss_control_treatment_assignment.groupby(
    ['logged_ss_pr_id', 'control_treatment_assignment']).size().reset_index(name='frequency')

randomized_pras = pr_ctrl_treat_grps.groupby(['logged_ss_pr_id']).size().reset_index(name='frequency')
randomized_pras_ = randomized_pras.loc[randomized_pras.frequency > 1]

pr_ctrl_treat_grps = pr_ctrl_treat_grps.loc[
    pr_ctrl_treat_grps.logged_ss_pr_id.isin(randomized_pras_.logged_ss_pr_id.unique())]

df_ss_control_treatment_assignment = df_ss_control_treatment_assignment.loc[
    df_ss_control_treatment_assignment.logged_ss_pr_id.isin(pr_ctrl_treat_grps.logged_ss_pr_id.unique())]

# looks like 880210 instances of ctrl treatment for CWAFs randomized (90:10 split)
# 869676 rows
# 1474 problem_ids
problemcounts = df_ss_control_treatment_assignment.logged_ss_pr_id.value_counts().\
    rename_axis('logged_ss_pr_id').reset_index(name='frequency')

problemcounts = problemcounts.loc[problemcounts.frequency >= 100]

# after filtering the >=100 limit
#   887775 rows
#   problem_ids: 1276
#   users: 23321
#   assignment: 9736
df_ss_control_treatment_assignment_ = df_ss_control_treatment_assignment.loc[
    df_ss_control_treatment_assignment.logged_ss_pr_id.isin(problemcounts.logged_ss_pr_id.unique())]
df_ss_control_treatment_assignment_.to_csv(
    "../data/rq3_Data/processed_from_ss/subsample_CWAF_randomized_ss_90_10_split_frequency_100_or_more.csv", index=False)

# without filtering the >=100 limit
#   897581 rows
#   problem_ids: 1464
#   users: 23503
#   assignment: 9904
df_ss_control_treatment_assignment.to_csv(
    "../data/rq3_Data/processed_from_ss/entire_CWAF_randomized_ss_90_10_split_frequency_100_or_more.csv", index=False)





