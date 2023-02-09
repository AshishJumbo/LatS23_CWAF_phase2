# dev-note:
#  this file merged the data together to compile the problem logs together in a single dataframe
#  the next preprocessing file will do something similar for the action logs

import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm

warnings.simplefilter(action='ignore', category=FutureWarning)

df_feedback_info = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_feedback_info.csv")
df_assignment_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_assignment_logs.csv")
df_assignment_logs_first_time = df_assignment_logs.groupby(
    ['student_xid', 'assignment_xid', 'assignment_log_id']).size().reset_index(name='frequency')
df_assignment_logs_first_time.sort_values(by=['student_xid', 'assignment_xid', 'assignment_log_id'], inplace=True)
df_assignment_logs_first_time.drop_duplicates(subset=['student_xid', 'assignment_xid'], keep='first', inplace=True)
df_assignment_logs = df_assignment_logs.loc[
    df_assignment_logs.assignment_log_id.isin(df_assignment_logs_first_time.assignment_log_id.unique())]
df_assignment_logs.reset_index(drop=True, inplace=True)
df_assignment_logs.sort_values(by=['student_xid', 'assignment_xid', 'assignment_log_id'], inplace=True)

df_problem_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_problem_logs.csv")
df_problem_logs = df_problem_logs.loc[
    df_problem_logs.assignment_log_id.isin(df_assignment_logs_first_time.assignment_log_id.unique())]
df_problem_logs.reset_index(drop=True, inplace=True)
df_action_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_action_logs.csv")
df_action_logs = df_action_logs.loc[
    df_action_logs.assignment_log_id.isin(df_assignment_logs_first_time.assignment_log_id.unique())]
df_action_logs.reset_index(drop=True, inplace=True)

# dev-note:
#  data from support service that was used to filter data in cas_core
#  just checking to see that the numbers add up
# df_1 = pd.read_csv(
#               '../data/rq3_Data/processed_from_ss/entire_CWAF_randomized_ss_90_10_split_frequency_100_or_more.csv')
# df_2 = pd.read_csv('../data/rq3_Data/raw_from_ss/CWAF_randomization_support_service_90_10_split.csv')
# test_Raw = df_1.groupby(['ss_user_xef', 'ss_assignment_xref']).size().reset_index(name='frequency')
# test_Processed = df_assignment_logs.groupby(['student_xid', 'assignment_xid']).size().reset_index(name='frequency')


# def generate_problem_logs_for_analysis(assignment_logs, problem_logs, feedback_info):
#     """
#         This function iterates through the problem_logs and checks for the treatment and control conditions. We also
#         check if their first response was a cwa and who wrote it.
#         :param assignment_logs:
#         :param problem_logs:
#         :param feedback_info:
#         :return: problem_logs with treatment control assignment in the randomization.
#     """
#     assignment_log_ids = assignment_logs.assignment_log_id.unique()
#     for assignment_log_id in tqdm(assignment_log_ids, desc="processing problem logs determining control treatment"):
#         temp_df = problem_logs.loc[problem_logs.assignment_log_id == assignment_log_id]
#         temp_df.sort_values(by=['problem_log_id'], inplace=True)
#         problem_log_ids = temp_df.problem_log_id.unique()
#         # problem_ids = temp_df.problem_id.unique()
#         experimental_problem_ids = assignment_logs.loc[
#             assignment_logs.assignment_log_id == assignment_log_id].logged_ss_pr_id.unique()
#         # print(experimental_problem_ids)
#         for problem_log_id in problem_log_ids:
#             problem_id = temp_df.loc[temp_df.problem_log_id == problem_log_id].problem_id.unique()[0]
#             if problem_id in experimental_problem_ids:
#                 control_treatment = assignment_logs.loc[
#                     (assignment_logs.assignment_log_id == assignment_log_id) &
#                     (assignment_logs.logged_ss_pr_id == problem_id)].control_treatment_assignment.unique()[0]
#                 problem_logs.loc[
#                     problem_logs.problem_log_id == problem_log_id, 'control_treatment_assignment'] = control_treatment
#                 if control_treatment == 'CWAF_treatment':
#                     CWAs = feedback_info.loc[feedback_info.problem_id == problem_id].cwa_response.unique()
#                     CWA_writer = feedback_info.loc[
#                         feedback_info.problem_id == problem_id].feedback_writer_id.unique()[0]
#                     answer_text = problem_logs.loc[
#                         problem_logs.problem_log_id == problem_log_id].answer_text.unique()[0].strip()
#                     # print('\n'+CWAs)
#                     # print(answer_text)
#                     if answer_text in CWAs:
#                         problem_logs.loc[
#                             problem_logs.problem_log_id == problem_log_id, 'common_wrong_answer'] = answer_text
#                     problem_logs.loc[
#                         problem_logs.problem_log_id == problem_log_id, 'CWA_writer'] = CWA_writer
#     return problem_logs
#
#
# df_problem_logs['control_treatment_assignment'] = 'Unassigned'
# df_problem_logs['common_wrong_answer'] = 'Unassigned'
# df_problem_logs['CWA_writer'] = -9999999
# df_problem_logs.answer_text.fillna('nan_answer_text', inplace=True)
# df_problem_logs = df_problem_logs[:1000]
# df_pr_log_rct_info = generate_problem_logs_for_analysis(df_assignment_logs, df_problem_logs, df_feedback_info)
# df_pr_log_rct_info.to_csv("../data/rq3_Data/processed_from_cas_core/processed_problem_logs.csv", index=False)

# dev-note: the above function takes for ever so ran is once and saved the df as a csv.
#  Can run it again if needed but reading the saved csv for now.
df_pr_log_rct_info = pd.read_csv("../data/rq3_Data/processed_from_cas_core/processed_problem_logs.csv")


# todo: loop through the problem_log with rct info and add next problem correctness data onto the dataframe
def generate_treatment_exposure_for_analysis(student_assignment_pair, df_pr_log_rct_info_):
    """
    This code loops through the problem log and computes
    (a)the order in which the learner was exposed to the treatment,
    (b)how far away was the last treatment,
    (c)what is the next problem correctness,
    (d)if in control were they ever in treatment
    :param student_assignment_pair:
    :param df_pr_log_rct_info_:
    :return:
    """
    student_xids = student_assignment_pair.student_xid.unique()
    for student_xid in tqdm(student_xids, desc='counting the prlogs and prior treatment assignment for all user'):
        assignment_log_ids = student_assignment_pair.loc[
            student_assignment_pair.student_xid == student_xid].assignment_log_id.unique()
        df_temp = df_pr_log_rct_info_.loc[df_pr_log_rct_info_.assignment_log_id.isin(assignment_log_ids)].groupby(
            ['problem_log_id', 'assignment_log_id', 'control_treatment_assignment']).size().reset_index(
            name="frequency")
        prev_assignment_log_id = df_pr_log_rct_info_.assignment_log_id[0]
        prior_treatment_count = 0
        within_assignment_prior_treatment_count = 0
        within_assignment_pr_count = 0
        # dev-note: because there was a context switch if student works on two assignments simultaneously the count
        #  is reset everytime they switch assignments. This is a possible edge case that we do not account here.
        for i in range(df_temp.shape[0]):
            assignment_log_id = df_temp.assignment_log_id[i]
            if prev_assignment_log_id != assignment_log_id:
                within_assignment_prior_treatment_count = 0
                within_assignment_pr_count = 0
            within_assignment_pr_count += 1
            control_treatment = df_temp.control_treatment_assignment[i]
            if control_treatment == 'CWAF_treatment':
                prior_treatment_count += 1
                within_assignment_prior_treatment_count += 1
            df_pr_log_rct_info_.loc[
                df_pr_log_rct_info_.problem_log_id == df_temp.problem_log_id[i],
                'prior_treatment_count'] = prior_treatment_count
            df_pr_log_rct_info_.loc[
                df_pr_log_rct_info_.problem_log_id == df_temp.problem_log_id[i],
                'within_assignment_prior_treatment_count'] = within_assignment_prior_treatment_count
            df_pr_log_rct_info_.loc[
                df_pr_log_rct_info_.problem_log_id == df_temp.problem_log_id[i],
                'within_assignment_pr_count'] = within_assignment_pr_count
            if (i + 1) < df_temp.shape[0] and assignment_log_id == df_temp.assignment_log_id[i + 1]:
                # df_pr_log_rct_info_.loc[
                #     df_pr_log_rct_info_.problem_log_id == df_temp.problem_log_id[i],
                #     'next_problem_correctness'] = df_pr_log_rct_info_.loc[
                #     df_pr_log_rct_info_.problem_log_id == df_temp.problem_log_id[i + 1]].continuous_score.unique()[0]
                df_pr_log_rct_info_.loc[df_pr_log_rct_info_.problem_log_id == df_temp.problem_log_id[i],
                                        'next_problem_log_id'] = df_temp.problem_log_id[i + 1]
            prev_assignment_log_id = assignment_log_id
    return df_pr_log_rct_info_


df_pr_log_rct_info['prior_treatment_count'] = -1
df_pr_log_rct_info['within_assignment_prior_treatment_count'] = -1
df_pr_log_rct_info['within_assignment_pr_count'] = -1
# df_pr_log_rct_info['next_problem_correctness'] = -1
df_pr_log_rct_info['next_problem_log_id'] = -1
df_student_al_pair = df_assignment_logs_first_time.loc[
    df_assignment_logs_first_time.assignment_log_id.isin(df_pr_log_rct_info.assignment_log_id.unique())]
df_pr_log_rct_info = generate_treatment_exposure_for_analysis(df_student_al_pair, df_pr_log_rct_info)
df_pr_log_rct_info.to_csv("../data/rq3_Data/processed_from_cas_core/processed_problem_logs_with_additional_info_2.csv",
                          index=False)
