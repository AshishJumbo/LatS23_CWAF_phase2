# dev-note:
#  this file merged the data together to compile the action logs together in a single dataframe

import pandas as pd
import numpy as np
import json

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

# dev-note: parse the student response if it was an attempt
df_action_logs['response'] = df_action_logs.extra_property_json.apply(
    lambda x: json.loads(x)['response'][0] if 'response' in json.loads(x) else 'no response/not attempt')

# dev-note: remove unnecessary al form the df
df_student_al_pair = df_assignment_logs_first_time.loc[
    df_assignment_logs_first_time.assignment_log_id.isin(df_problem_logs.assignment_log_id.unique())]
df_assignment_logs = df_assignment_logs.loc[
    df_assignment_logs.assignment_log_id.isin(df_student_al_pair.assignment_log_id)]
df_action_logs = df_action_logs.merge(
    df_assignment_logs[['assignment_log_id', 'logged_ss_pr_id', 'assignment_xid', 'control_treatment_assignment']],
    how='left',
    left_on=['assignment_log_id', 'problem_id'],
    right_on=['assignment_log_id', 'logged_ss_pr_id'])
df_action_logs = df_action_logs.merge(
    df_feedback_info[['problem_id', 'feedback_writer_id', 'cwa_response', 'frequency_of_cwa', 'feedback_message']],
    how='left',
    left_on=['problem_id', 'response'],
    right_on=['problem_id', 'cwa_response'])

# df_action_logs = df_action_logs[['user_xid', 'problem_id', 'id', 'timestamp', 'action_type_id',
#                                  'target_xref', 'path', 'assignment_log_id', 'tutor_uid_id',
#                                  'tutor_uid_sequence', 'extra_property_json', 'parsed_pra',
#                                  'action_type', 'previous_action_time', 'action_pairs', 'next_action',
#                                  'time_difference_timestamp', 'time_difference_seconds', 'response',
#                                  'control_treatment_assignment', 'feedback_writer_id',
#                                  'frequency_of_cwa', 'feedback_message']]
potential_treated_instances = df_action_logs.loc[~df_action_logs.feedback_message.isna()]
assigned_treated_instances = df_action_logs.loc[
    df_action_logs.control_treatment_assignment.isin(['CWAF_treatment', 'control'])]
truly_treated_instances = potential_treated_instances.loc[
    ~potential_treated_instances.control_treatment_assignment.isna()]

# dev-note: let us pull out the next problem correctness from the processed problem logs and add them to the
#  treated group
# important:the next problem correctness is -1 if there is not next problem for the assignment but nan if
#  there was a next problem but it didn't have a grade
df_pr_log_processed = pd.read_csv(
    "../data/rq3_Data/processed_from_cas_core/processed_problem_logs_with_additional_info.csv")
df_student_al_pair = df_assignment_logs_first_time.loc[
    df_assignment_logs_first_time.assignment_log_id.isin(df_pr_log_processed.assignment_log_id.unique())]

df_pr_log_processed = df_pr_log_processed.merge(
    df_student_al_pair[['assignment_log_id', 'student_xid', 'assignment_xid']],
    how='left',
    on=['assignment_log_id'])
df_pr_log_processed.sort_values(by=['student_xid', 'problem_log_id'], inplace=True)
df_pr_log_processed.reset_index(drop=True, inplace=True)

# df_pr_log_processed = df_pr_log_processed[['assignment_xid', 'student_xid', 'assignment_log_id', 'problem_log_id',
#                                            'problem_id', 'start_time',  'end_time', 'continuous_score', 'answer_text',
#                                            'first_action_type_id', 'attempt_count', 'hint_count', 'bottom_hint',
#                                            'first_response_time', 'teacher_comment', 'problem_type_id',
#                                            'control_treatment_assignment', 'common_wrong_answer', 'CWA_writer',
#                                            'prior_treatment_count', 'within_assignment_prior_treatment_count',
#                                            'within_assignment_pr_count', 'next_problem_correctness']]
# df_pr_log_processed = df_pr_log_processed[['assignment_xid', 'student_xid', 'assignment_log_id', 'problem_log_id',
#                                            'problem_id', 'control_treatment_assignment', 'prior_treatment_count',
#                                            'within_assignment_prior_treatment_count', 'within_assignment_pr_count',
#                                            'next_problem_correctness', 'continuous_score', 'answer_text',
#                                            'first_action_type_id', 'attempt_count', 'hint_count', 'bottom_hint',
#                                            'first_response_time', 'problem_type_id', 'common_wrong_answer',
#                                            'CWA_writer']]

# df_pr_log_processed = df_pr_log_processed.loc[df_pr_log_processed.problem_type_id != 8]

truly_treated_instances_value_counts = truly_treated_instances.groupby(
    ['assignment_log_id', 'user_xid', 'problem_id', 'control_treatment_assignment']
).size().reset_index(name='frequency')
df_pr_log_processed = df_pr_log_processed.merge(
    truly_treated_instances_value_counts, how='inner',
    on=['assignment_log_id', 'problem_id', 'control_treatment_assignment'])
df_pr_log_processed.to_csv("../data/rq3_Data/processed_from_cas_core/processed_pr_logs_treated_analysis.csv", index=False)





# dev-note: now that we have info on the learners who were actually exposed to treatment
#  let's count total exposure to treatment and control per problem across all students
# counting_user_pr_rct = truly_treated_instances.groupby(
#     ['assignment_xid', 'assignment_log_id', 'user_xid', 'problem_id', 'control_treatment_assignment']) \
#     .size().reset_index(name='frequency')
# counting_problem_rct = counting_user_pr_rct.groupby(
#     ['problem_id', 'control_treatment_assignment']).size().reset_index(name='frequency')
# counting_within_assignment_log_id_rct = counting_user_pr_rct.groupby(
#     ['assignment_log_id', 'control_treatment_assignment']).size().reset_index(name='frequency')
# counting_within_assignment_log_id_exposure = counting_user_pr_rct.groupby(
#     ['assignment_log_id', 'control_treatment_assignment'])['problem_id'].agg(['unique', 'size']).reset_index()
# counting_within_assignment_pr_rct = counting_user_pr_rct.groupby(
#     ['assignment_xid', 'problem_id', 'control_treatment_assignment']).agg(['size']).reset_index()
# counting_within_assignment_pr_rct = counting_user_pr_rct.groupby(
#     ['assignment_xid', 'control_treatment_assignment'])['assignment_log_id'].agg(['unique', 'size']).reset_index()



