import pandas as pd
import statsmodels.formula.api as smf
import scipy.stats as stats

# dev-note:
#  let's generate the user_xid for the assignment_log_ids
df_problem_logs = pd.read_csv(
    '../data/rq3_Data/processed_from_cas_core/processed_problem_logs_with_additional_info.csv')
df_problem_logs.sort_values(by=
                            ['student_xid', "problem_id", "problem_log_id"],
                            inplace=True)
df_problem_logs.drop_duplicates(subset=['student_xid', "problem_id"], keep='first', inplace=True)

df_assignment_logs = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_phase2_analysis_assignment_logs.csv")
df_assignment_logs_first_time = df_assignment_logs.groupby(
    ['student_xid', 'assignment_xid', 'assignment_log_id']).size().reset_index(name='frequency')
df_assignment_logs_first_time.sort_values(by=['student_xid', 'assignment_xid', 'assignment_log_id'], inplace=True)
df_assignment_logs_first_time.drop_duplicates(subset=['student_xid', 'assignment_xid'], keep='first', inplace=True)
df_student_al_pair = df_assignment_logs_first_time.loc[
    df_assignment_logs_first_time.assignment_log_id.isin(df_problem_logs.assignment_log_id.unique())]

df_problem_logs = df_problem_logs.merge(df_student_al_pair[['assignment_log_id', 'student_xid', 'assignment_xid']],
                                        how='left', on=['assignment_log_id'])

# important we analyze with aggregated observation at
#  1. problem level and
#  2. student-level
#  ----for both analysis we will do a intent to treat and treated analysis----
#  ===Intent to treat===
#  the depended measures for intent to treat are
#  1. next problem correctness binary
#  2. help request
#  3. attempt count
#  4. ?stop out
#  -------------------------------
#  ===treated===
#  the depended measures for treated are
#  1. next problem correctness binary
#  2. help request
#  3. attempt count
#  4. ?stop out
#  ===with in problem next action===
#  1. next action correctness
#  2. next action help request
#  3. ?stop out
#  --------------------------------


# IMPORTANT:
#  =============================
#  INTENT TO TREAT
#  =============================
# def conduct_pr_agg_paired_ttest(
#         df,
#         target_feature='next_problem_correctness',
#         npc_discrete=False,
#         sample_n_threshold=5):
#     df = df.loc[~df.next_problem_correctness.isin([-999, -1])]
#     df[target_feature+'_binary'] = 0
#     df[target_feature+'_binary'] = df[target_feature].apply(
#         lambda x: 1 if x == 1 else 0)
#     if npc_discrete:
#         problem_rct_condn = df.groupby(['problem_id', 'control_treatment_assignment'])[
#             'next_problme_correctness_binary'].agg(['size', 'mean']).reset_index()
#     else:
#         problem_rct_condn = df.groupby(['problem_id', 'control_treatment_assignment'])[
#             'next_problem_correctness'].agg(['size', 'mean']).reset_index()
#     # filter out the problems that have n < 5 in condition
#     problem_rct_condn_gt_threshold_n = problem_rct_condn.loc[problem_rct_condn['size'] >= sample_n_threshold]
#     # we can only analyze problems that have bot CWAF_treatment and control on the problem
#     eligible_prs = problem_rct_condn_gt_threshold_n.groupby(['problem_id']).size().reset_index(name='frequency')
#     eligible_prs = eligible_prs.loc[eligible_prs.frequency > 1]
#
#     problem_rct_condn_gt_threshold_n = problem_rct_condn_gt_threshold_n.loc[
#         problem_rct_condn_gt_threshold_n.problem_id.isin(eligible_prs.problem_id.unique())]
#     pr_ids = eligible_prs.problem_id.unique()
#
#     df_problem_agg = pd.DataFrame(pr_ids, columns=['problem_id'])
#     df_problem_agg['treatment_mean'] = 0
#     df_problem_agg['treatment_count'] = 0
#     df_problem_agg['control_mean'] = 0
#     df_problem_agg['control_count'] = 0
#
#     for pr_id in pr_ids:
#         df_problem_agg.loc[
#             df_problem_agg.problem_id == pr_id,
#             'treatment_mean'] = problem_rct_condn_gt_threshold_n.loc[
#             (problem_rct_condn_gt_threshold_n.problem_id == pr_id) &
#             (problem_rct_condn_gt_threshold_n.control_treatment_assignment == 'CWAF_treatment')]['mean'].unique()[0]
#         df_problem_agg.loc[
#             df_problem_agg.problem_id == pr_id,
#             'treatment_count'] = problem_rct_condn_gt_threshold_n.loc[
#             (problem_rct_condn_gt_threshold_n.problem_id == pr_id) &
#             (problem_rct_condn_gt_threshold_n.control_treatment_assignment == 'CWAF_treatment')]['size'].unique()[0]
#         df_problem_agg.loc[
#             df_problem_agg.problem_id == pr_id,
#             'control_mean'] = problem_rct_condn_gt_threshold_n.loc[
#             (problem_rct_condn_gt_threshold_n.problem_id == pr_id) &
#             (problem_rct_condn_gt_threshold_n.control_treatment_assignment == 'control')]['mean'].unique()[0]
#         df_problem_agg.loc[
#             df_problem_agg.problem_id == pr_id,
#             'control_count'] = problem_rct_condn_gt_threshold_n.loc[
#             (problem_rct_condn_gt_threshold_n.problem_id == pr_id) &
# #             (problem_rct_condn_gt_threshold_n.control_treatment_assignment == 'control')]['size'].unique()[0]
#
#     print('===================================')
#     print('user level aggregation with no correction')
#     print('at a sample n threshold of:', sample_n_threshold, ' we have:', eligible_prs.shape[0],
#           ' eligible problems.')
#     print(stats.ttest_rel(df_problem_agg.treatment_mean, df_problem_agg.control_mean))
#     print('===================================')
#     return df_problem_agg


df_problem_logs = df_problem_logs.loc[df_problem_logs.control_treatment_assignment.isin(['CWAF_treatment', 'control'])]
df_problem_logs.next_problem_correctness.fillna(-999, inplace=True)
# dev-note: 999 means they dropped out in the middle of a problem
# df_pr_agg = conduct_pr_agg_paired_ttest(df_problem_logs)
df_problem_logs = df_problem_logs.loc[~df_problem_logs.next_problem_correctness.isin([-999, -1])]
df_problem_logs['next_problem_correctness_binary'] = 0
df_problem_logs['next_problem_correctness_binary'] = df_problem_logs['next_problem_correctness'].apply(
    lambda x: 1 if x == 1 else 0)


def conduct_aggregated_paired_ttest(
        df,
        target_feature='next_problem_correctness',
        aggregation_level='problem_id',
        sample_n_threshold=5):
    """
    this function conducts the uncorrected t-test exploring the effectivness of CWAFs as a treatment aggregated at a
    problem level and user level.
    :param df:
    :param target_feature:
    :param aggregation_level: what is the aggregation level? problem_id or student_xid
    :param sample_n_threshold:
    :return:
    """
    aggregated_rct_condition = df.groupby(
        [aggregation_level, 'control_treatment_assignment'])[target_feature].agg(['size', 'mean']).reset_index()
    # filter out the problems that have n < 5 in condition
    agg_rct_gt_threshold_n = aggregated_rct_condition.loc[aggregated_rct_condition['size'] >= sample_n_threshold]
    # we can only analyze problems that have bot CWAF_treatment and control on the problem
    eligible_targets = agg_rct_gt_threshold_n.groupby([aggregation_level]).size().reset_index(name='frequency')
    eligible_targets = eligible_targets.loc[eligible_targets.frequency > 1]

    agg_rct_gt_threshold_n = agg_rct_gt_threshold_n.loc[
        agg_rct_gt_threshold_n[aggregation_level].isin(eligible_targets[aggregation_level].unique())]
    target_ids = eligible_targets[aggregation_level].unique()

    df_problem_agg = pd.DataFrame(target_ids, columns=[aggregation_level])
    df_problem_agg['treatment_mean'] = 0
    df_problem_agg['treatment_count'] = 0
    df_problem_agg['control_mean'] = 0
    df_problem_agg['control_count'] = 0

    for target_id in target_ids:
        df_problem_agg.loc[
            df_problem_agg[aggregation_level] == target_id,
            'treatment_mean'] = agg_rct_gt_threshold_n.loc[
            (agg_rct_gt_threshold_n[aggregation_level] == target_id) &
            (agg_rct_gt_threshold_n.control_treatment_assignment == 'CWAF_treatment')]['mean'].unique()[0]
        df_problem_agg.loc[
            df_problem_agg[aggregation_level] == target_id,
            'treatment_count'] = agg_rct_gt_threshold_n.loc[
            (agg_rct_gt_threshold_n[aggregation_level] == target_id) &
            (agg_rct_gt_threshold_n.control_treatment_assignment == 'CWAF_treatment')]['size'].unique()[0]
        df_problem_agg.loc[
            df_problem_agg[aggregation_level] == target_id,
            'control_mean'] = agg_rct_gt_threshold_n.loc[
            (agg_rct_gt_threshold_n[aggregation_level] == target_id) &
            (agg_rct_gt_threshold_n.control_treatment_assignment == 'control')]['mean'].unique()[0]
        df_problem_agg.loc[
            df_problem_agg[aggregation_level] == target_id,
            'control_count'] = agg_rct_gt_threshold_n.loc[
            (agg_rct_gt_threshold_n[aggregation_level] == target_id) &
            (agg_rct_gt_threshold_n.control_treatment_assignment == 'control')]['size'].unique()[0]

    print('===================================')
    print(target_feature, '\n')
    print('user level aggregation with no correction')
    print('at a sample n threshold of:', sample_n_threshold, ' we have:', eligible_targets.shape[0],
          ' eligible ', aggregation_level)
    print(sample_n_threshold, ': ', stats.ttest_rel(df_problem_agg.treatment_mean, df_problem_agg.control_mean))
    print('===================================')
    return df_problem_agg, agg_rct_gt_threshold_n


df_1, df_filtered1 = conduct_aggregated_paired_ttest(df_problem_logs)
df_2, df_filtered2 = conduct_aggregated_paired_ttest(df_problem_logs, sample_n_threshold=10)
df_3, df_filtered3 = conduct_aggregated_paired_ttest(
    df_problem_logs,
    # target_feature='next_problem_correctness_binary',
    aggregation_level='student_xid')
df_4, df_filtered4 = conduct_aggregated_paired_ttest(
    df_problem_logs, aggregation_level='student_xid', sample_n_threshold=10)

# df_1.to_csv("../data/rq3_Data/export_for_R/itt_problem_gt_5_aggregated_data.csv", index=False)
# df_3.to_csv("../data/rq3_Data/export_for_R/itt_user_gt_5_aggregated_data.csv", index=False)
# df_filtered1.to_csv("../data/rq3_Data/export_for_R/itt_unflattened_problem_gt_5_aggregated_data.csv", index=False)
# df_filtered3.to_csv("../data/rq3_Data/export_for_R/itt_unflattened_user_gt_5_aggregated_data.csv", index=False)
# df_problem_logs.to_csv("../data/rq3_Data/export_for_R/itt_pr_logs.csv", index=False)

# TODO:  analyze the experiment for interpretation of the effectsize using lipsy's interpretation
