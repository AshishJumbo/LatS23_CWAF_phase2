import pandas as pd
import statsmodels.formula.api as smf
import scipy.stats as stats

df_problem_logs = pd.read_csv("../data/rq3_Data/processed_from_cas_core/processed_pr_logs_treated_analysis.csv")
df_problem_logs.sort_values(by=
                            ['student_xid', "problem_id", "problem_log_id"],
                            inplace=True)
df_problem_logs.drop_duplicates(subset=['student_xid', "problem_id"], keep='first', inplace=True)

# important we analyze with aggregated observation at
#  1. problem level and
#  2. student-level
#  ----for both analysis we will do a intent to treat and treated analysis----
#  ---------------------------------------------------------------------------
#  ===treated===
#  the depended measures for treated are
#  1. next problem correctness binary
#  ===with in problem next action===
#  1. next action correctness
#  --------------------------------


# IMPORTANT:
#  =============================
#  TREATED ANALYSIS
#  =============================
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


df_problem_logs = df_problem_logs.loc[df_problem_logs.control_treatment_assignment.isin(['CWAF_treatment', 'control'])]
df_problem_logs.next_problem_correctness.fillna(-999, inplace=True)
df_problem_logs = df_problem_logs.loc[~df_problem_logs.next_problem_correctness.isin([-999, -1])]
df_problem_logs['next_problem_correctness_binary'] = 0
df_problem_logs['next_problem_correctness_binary'] = df_problem_logs.next_problem_correctness.apply(
    lambda x: 1 if x == 1 else 0)


count_threshold = 5
df_1, df_filtered1 = conduct_aggregated_paired_ttest(df_problem_logs)
df_2, df_filtered2 = conduct_aggregated_paired_ttest(df_problem_logs, sample_n_threshold=10)
df_0, df_filtered0 = conduct_aggregated_paired_ttest(df_problem_logs, sample_n_threshold=0)
# conduct_aggregated_paired_ttest(df_problem_logs, sample_n_threshold=15)
# conduct_aggregated_paired_ttest(df_problem_logs, sample_n_threshold=20)
df_3, df_filtered3 = conduct_aggregated_paired_ttest(
    df_problem_logs, target_feature='next_problem_correctness_binary', aggregation_level='student_xid')
df_4, df_filtered4 = conduct_aggregated_paired_ttest(
    df_problem_logs, aggregation_level='student_xid', sample_n_threshold=10)
conduct_aggregated_paired_ttest(df_problem_logs, sample_n_threshold=0)
conduct_aggregated_paired_ttest(
    df_problem_logs, aggregation_level='student_xid', sample_n_threshold=0)

df_1.to_csv("../data/rq3_Data/export_for_R/treated_problem_gt_5_aggregated_data.csv", index=False)
df_3.to_csv("../data/rq3_Data/export_for_R/treated_user_gt_5_aggregated_data.csv", index=False)
df_filtered1.to_csv("../data/rq3_Data/export_for_R/treated_unflattened_problem_gt_5_aggregated_data.csv", index=False)
df_filtered3.to_csv("../data/rq3_Data/export_for_R/treated_unflattened_user_gt_5_aggregated_data.csv", index=False)

df_prior_kng_data = pd.read_csv("../data/rq3_Data/raw_from_cas_core/CWAF_prior_kng_data.csv")
df_problem_logs = df_problem_logs.merge(
    df_prior_kng_data, how='left', on=['problem_log_id', 'problem_id', 'student_xid'])

df_assignment_class_map = pd.read_csv("../data/rq3_Data/raw_from_cas_core/assignment_xid_class_teacher_map.csv")
df_problem_logs = df_problem_logs.merge(df_assignment_class_map, how='left', on=['assignment_xid'])



df_problem_logs.loc[df_problem_logs.control_treatment_assignment == 'control', "CWA_writer"] = "_control"

df_problem_logs_0 = df_problem_logs.loc[df_problem_logs.problem_id.isin(df_0.problem_id.unique())]
df_problem_logs_5 = df_problem_logs.loc[df_problem_logs.problem_id.isin(df_1.problem_id.unique())]
df_problem_logs_10 = df_problem_logs.loc[df_problem_logs.problem_id.isin(df_2.problem_id.unique())]
df_problem_logs_0.to_csv("../data/rq3_Data/export_for_R/treated_pr_logs_0.csv", index=False)
df_problem_logs_0_ = df_problem_logs_0[['problem_log_id', 'assignment_log_id', 'problem_id',
                                        'CWA_writer', 'student_xid', 'assignment_xid']]
df_problem_logs_0_.to_csv("../data/rq3_Data/assignment_xid_to_get_classid.csv", index=False)

# df_problem_logs_5.to_csv("../data/rq3_Data/export_for_R/treated_pr_logs_5.csv", index=False)
# df_problem_logs_10.to_csv("../data/rq3_Data/export_for_R/treated_pr_logs_10.csv", index=False)
# df_problem_logs.to_csv("../data/rq3_Data/export_for_R/treated_pr_logs.csv", index=False)

df_assistments_problem_map = pd.read_csv("../data/rq3_Data/raw_from_cas_core/assistments_problem_map.csv")
df_assistments_problem_map = df_assistments_problem_map[
    ['problem_id', 'assistment_id', 'position', 'max_positions']]
df_assistments_problem_map = df_assistments_problem_map.loc[
    df_assistments_problem_map.position < df_assistments_problem_map.max_positions]
df_problem_logs_within_assistments = df_problem_logs_0.loc[
    df_problem_logs_0.problem_id.isin(df_assistments_problem_map.problem_id.unique())]
df_problem_logs_within_assistments.to_csv(
    "../data/rq3_Data/export_for_R/treated_pr_logs_0_within_assistments.csv", index=False)

# np ~ z + pr_id + user_id + running_avg_prior_p

#
# problem_rct_condition = df_problem_logs.groupby(['problem_id', 'control_treatment_assignment']
#                                                 )['next_problem_correctness'].agg(['size', 'mean']).reset_index()
#
# problem_rct_condition_gt_5 = problem_rct_condition.loc[problem_rct_condition['size'] >= count_threshold]
# eligible_prs = problem_rct_condition_gt_5.groupby(['problem_id']).size().reset_index(name='frequency')
# eligible_prs = eligible_prs.loc[eligible_prs.frequency > 1]
#
# # dev-note: 1176 total problems with frequency > 5
# problem_rct_condition_gt_5_filtered = problem_rct_condition_gt_5.loc[
#     problem_rct_condition_gt_5.problem_id.isin(eligible_prs.problem_id.unique())]
# problem_ids = eligible_prs.problem_id.unique()
#
# df_problem_agg = pd.DataFrame(problem_ids, columns=['problem_id'])
# df_problem_agg['treatment_mean'] = 0
# df_problem_agg['treatment_count'] = 0
# df_problem_agg['control_mean'] = 0
# df_problem_agg['control_count'] = 0
#
# for problem_id in problem_ids:
#     df_problem_agg.loc[
#         df_problem_agg.problem_id == problem_id,
#         'treatment_mean'] = problem_rct_condition_gt_5_filtered.loc[
#         (problem_rct_condition_gt_5_filtered.problem_id == problem_id) &
#         (problem_rct_condition_gt_5_filtered.control_treatment_assignment == 'CWAF_treatment')]['mean'].unique()[0]
#     df_problem_agg.loc[
#         df_problem_agg.problem_id == problem_id,
#         'treatment_count'] = problem_rct_condition_gt_5_filtered.loc[
#         (problem_rct_condition_gt_5_filtered.problem_id == problem_id) &
#         (problem_rct_condition_gt_5_filtered.control_treatment_assignment == 'CWAF_treatment')]['size'].unique()[0]
#     df_problem_agg.loc[
#         df_problem_agg.problem_id == problem_id,
#         'control_mean'] = problem_rct_condition_gt_5_filtered.loc[
#         (problem_rct_condition_gt_5_filtered.problem_id == problem_id) &
#         (problem_rct_condition_gt_5_filtered.control_treatment_assignment == 'control')]['mean'].unique()[0]
#     df_problem_agg.loc[
#         df_problem_agg.problem_id == problem_id,
#         'control_count'] = problem_rct_condition_gt_5_filtered.loc[
#         (problem_rct_condition_gt_5_filtered.problem_id == problem_id) &
#         (problem_rct_condition_gt_5_filtered.control_treatment_assignment == 'control')]['size'].unique()[0]
#
# # dev-note:
# #  paired sample t-test
# #  let's look at the problem-level aggregation
# print('problem level aggregation with no correction')
# print(stats.ttest_rel(df_problem_agg.treatment_mean, df_problem_agg.control_mean))
# print('\n=====================================================================')
#
# # ====================================================
# user_rct_condition = df_problem_logs.groupby(['student_xid', 'control_treatment_assignment']
#                                              )['next_problem_correctness_binary'].agg(['size', 'mean']).reset_index()
# user_rct_condition_gt_5 = user_rct_condition.loc[user_rct_condition['size'] >= count_threshold]
# eligible_users = user_rct_condition_gt_5.groupby(['student_xid']).size().reset_index(name='frequency')
# eligible_users = eligible_users.loc[eligible_users.frequency > 1]
#
# # dev-note: 3796 users were exposed to both treatment and control atleast 5 times
#
# user_rct_condition_gt_5_filtered = user_rct_condition_gt_5.loc[
#     user_rct_condition_gt_5.student_xid.isin(eligible_users.student_xid.unique())]
# student_xids = eligible_users.student_xid.unique()
#
# df_user_agg = pd.DataFrame(student_xids, columns=['student_xid'])
# df_user_agg['treatment_mean'] = 0
# df_user_agg['treatment_count'] = 0
# df_user_agg['control_mean'] = 0
# df_user_agg['control_count'] = 0
#
# for student_xid in student_xids:
#     df_user_agg.loc[
#         df_user_agg.student_xid == student_xid,
#         'treatment_mean'] = user_rct_condition_gt_5_filtered.loc[
#         (user_rct_condition_gt_5_filtered.student_xid == student_xid) &
#         (user_rct_condition_gt_5_filtered.control_treatment_assignment == 'CWAF_treatment')]['mean'].unique()[0]
#     df_user_agg.loc[
#         df_user_agg.student_xid == student_xid,
#         'treatment_count'] = user_rct_condition_gt_5_filtered.loc[
#         (user_rct_condition_gt_5_filtered.student_xid == student_xid) &
#         (user_rct_condition_gt_5_filtered.control_treatment_assignment == 'CWAF_treatment')]['size'].unique()[0]
#     df_user_agg.loc[
#         df_user_agg.student_xid == student_xid,
#         'control_mean'] = user_rct_condition_gt_5_filtered.loc[
#         (user_rct_condition_gt_5_filtered.student_xid == student_xid) &
#         (user_rct_condition_gt_5_filtered.control_treatment_assignment == 'control')]['mean'].unique()[0]
#     df_user_agg.loc[
#         df_user_agg.student_xid == student_xid,
#         'control_count'] = user_rct_condition_gt_5_filtered.loc[
#         (user_rct_condition_gt_5_filtered.student_xid == student_xid) &
#         (user_rct_condition_gt_5_filtered.control_treatment_assignment == 'control')]['size'].unique()[0]
#
# # dev-note:
# #  paired sample t-test
# #  let's look at the user-level aggregation
# print('user level aggregation with no correction')
# print(stats.ttest_rel(df_user_agg.treatment_mean, df_user_agg.control_mean))
# print('\n=====================================================================')


# model = smf.ols(formula='treatment_mean ~ control_mean', data=df_problem_agg).fit()
# print(model.summary())

# npc ~ treats://help.xlstat.com/6588-nonparametric-regression-kernel-lowess-tutorial

