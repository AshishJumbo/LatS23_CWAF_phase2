import pandas as pd
import numpy as np

# Warning: Problem_id in this analysis is actually pra_id/assistment id rendered into
#  string and question_id is real problem id

problem_types = ['Ungraded Open Response', 'Multiple Choice', 'Check All That Apply']

df_17_18_raw = pd.read_csv("../data/rq1_Data/ASSISTments_17_18_CWA_v1.csv")
df_18_19_raw = pd.read_csv("../data/rq1_Data/ASSISTments_18_19_CWA_v1.csv")
df_19_20_raw = pd.read_csv("../data/rq1_Data/ASSISTments_19_20_CWA_v1.csv")
df_20_21_raw = pd.read_csv("../data/rq1_Data/ASSISTments_16_17_CWA_v1.csv")
df_21_22_raw = pd.read_csv("../data/rq1_Data/ASSISTments_15_16_CWA_v1.csv")
df_17_18_raw = df_17_18_raw.loc[df_17_18_raw.number_of_students > 20]
df_17_18_raw['academic_year'] = '17_18'
df_18_19_raw = df_18_19_raw.loc[df_18_19_raw.number_of_students > 20]
df_18_19_raw['academic_year'] = '18_19'
df_19_20_raw = df_19_20_raw.loc[df_19_20_raw.number_of_students > 20]
df_19_20_raw['academic_year'] = '19_20'
df_20_21_raw = df_20_21_raw.loc[df_20_21_raw.number_of_students > 20]
df_20_21_raw['academic_year'] = '16_17'
df_21_22_raw = df_21_22_raw.loc[df_21_22_raw.number_of_students > 20]
df_21_22_raw['academic_year'] = '15_16'
df_raw = pd.concat([df_17_18_raw, df_18_19_raw, df_19_20_raw, df_20_21_raw, df_21_22_raw], ignore_index=True)
df_counts_raw = df_raw.groupby(['academic_year', 'path', 'path.1', 'question_id']).size().reset_index(name='frequency')
df_counts_raw = df_counts_raw.groupby(['path', 'path.1', 'question_id']).size().reset_index(name='frequency')
df_counts_raw = df_counts_raw.groupby(['path', 'path.1']).size().reset_index(name='frequency')
df_counts_raw.sort_values(by=["path", "path.1"], inplace=True)


def generate_info(path):
    df_ = pd.read_csv('../data/rq1_Data/ASSISTments_' + path + '_CWA_v1.csv')
    df_['incorrect_count'] = np.ceil(((100 - df_.percent_correct) / 100) * df_.number_of_students)
    df_ = df_.loc[((df_.number_of_students >= 20) & (df_.first_common_wrong_answer_count > 5) &
                   (~df_.problem_type.isin(problem_types)))]
    psa_ = set(df_.sequence_id.unique())
    problem_ = set(df_.question_id.unique())  # not pra but question_id
    df_['academic_year'] = path
    return df_, psa_, problem_


df_17_18, psa_17_18, problem_17_18 = generate_info('17_18')
df_18_19, psa_18_19, problem_18_19 = generate_info('18_19')
df_19_20, psa_19_20, problem_19_20 = generate_info('19_20')
df_20_21, psa_20_21, problem_20_21 = generate_info('16_17')
df_21_22, psa_21_22, problem_21_22 = generate_info('15_16')

# duplicate_psa_17_22 = psa_17_18 & psa_18_19 & psa_19_20 & psa_20_21 & psa_21_22
duplicate_problem_17_22 = (problem_17_18 & problem_18_19) | (problem_18_19 & problem_19_20) | \
                          (problem_19_20 & problem_20_21) | (problem_20_21 & problem_21_22)



df_17_18 = df_17_18.loc[(#(df_17_18.sequence_id.isin(duplicate_psa_17_22)) &
                         (df_17_18.question_id.isin(duplicate_problem_17_22)))]
df_18_19 = df_18_19.loc[(#(df_18_19.sequence_id.isin(duplicate_psa_17_22)) &
                         (df_18_19.question_id.isin(duplicate_problem_17_22)))]
df_19_20 = df_19_20.loc[(#(df_19_20.sequence_id.isin(duplicate_psa_17_22)) &
                         (df_19_20.question_id.isin(duplicate_problem_17_22)))]
df_20_21 = df_20_21.loc[(#(df_20_21.sequence_id.isin(duplicate_psa_17_22)) &
                         (df_20_21.question_id.isin(duplicate_problem_17_22)))]
df_21_22 = df_21_22.loc[(#(df_21_22.sequence_id.isin(duplicate_psa_17_22)) &
                         (df_21_22.question_id.isin(duplicate_problem_17_22)))]

columns = [
    'path', 'academic_year', 'sequence_id', 'sequence_name',  # 'skill_strand',
    'problem_id', 'question_id',
    'problem_type', 'type', 'number_of_students', 'incorrect_count', 'percent_correct', 'correct_answer',
    'average_time', 'first_common_wrong_answer', 'first_common_wrong_answer_count', 'second_common_wrong_answer',
    'second_common_wrong_answer_count', 'third_common_wrong_answer', 'third_common_wrong_answer_count',
    'fourth_common_wrong_answer', 'fourth_common_wrong_answer_count', 'fifth_common_wrong_answer',
    'fifth_common_wrong_answer_count'
]

df_17_18 = df_17_18[columns]
df_18_19 = df_18_19[columns]
df_19_20 = df_19_20[columns]
df_20_21 = df_20_21[columns]
df_21_22 = df_21_22[columns]

print(df_17_18.path.value_counts())
print(df_18_19.path.value_counts())
print(df_19_20.path.value_counts())
print(df_20_21.path.value_counts())
print(df_21_22.path.value_counts())

df_17_18 = df_17_18.sort_values(by=['first_common_wrong_answer_count'], ascending=False)
df_18_19 = df_18_19.sort_values(by=['first_common_wrong_answer_count'], ascending=False)
df_19_20 = df_19_20.sort_values(by=['first_common_wrong_answer_count'], ascending=False)
df_20_21 = df_20_21.sort_values(by=['first_common_wrong_answer_count'], ascending=False)
df_21_22 = df_21_22.sort_values(by=['first_common_wrong_answer_count'], ascending=False)

df_17_18.reset_index(drop=True, inplace=True)
df_18_19.reset_index(drop=True, inplace=True)
df_19_20.reset_index(drop=True, inplace=True)
df_20_21.reset_index(drop=True, inplace=True)
df_21_22.reset_index(drop=True, inplace=True)

# df_17_18 = df_17_18[:150]
# df_18_19 = df_18_19[:150]
# df_19_20 = df_19_20[:150]
# df_20_21 = df_20_21[:150]
# df_21_22 = df_21_22[:150]


columns = [
    'path', 'sequence_name',  'academic_year', 'sequence_id', 'problem_id', 'question_id',
    'problem_type', 'type', 'number_of_students', 'incorrect_count', 'percent_correct', 'correct_answer',
    'average_time', 'first_common_wrong_answer', 'second_common_wrong_answer', 'third_common_wrong_answer',
    'first_common_wrong_answer_count', 'second_common_wrong_answer_count', 'third_common_wrong_answer_count'
]
df = pd.concat([df_17_18, df_18_19, df_19_20, df_20_21, df_21_22], ignore_index=True)
df.sort_values(by=['problem_id', 'question_id', 'academic_year'], inplace=True)
df = df[columns]

common_pr_ids = problem_17_18.intersection(problem_18_19.intersection(problem_19_20.intersection(problem_20_21)))


df_common_pr_ids = df.loc[df.question_id.isin(list(common_pr_ids))]


df_ = df[['path', 'sequence_name', 'problem_id', 'question_id', 'academic_year', 'sequence_id', 'number_of_students',
          'incorrect_count', 'percent_correct', 'correct_answer', 'first_common_wrong_answer',
          'second_common_wrong_answer', 'third_common_wrong_answer', 'first_common_wrong_answer_count',
          'second_common_wrong_answer_count', 'third_common_wrong_answer_count']]
df_.sort_values(by=["path", "sequence_name", 'problem_id', 'question_id', "academic_year"], inplace=True)


# df_raw_ = df_raw.loc[df_raw.question_id.isin(list(duplicate_problem_17_22))]
# df_counts_raw = df_raw_.groupby(['academic_year', 'path', 'path.1', 'question_id']).size().reset_index(name='frequency')
# df_counts_raw = df_counts_raw.groupby(['path', 'path.1', "question_id"]).size().reset_index(name='frequency')
# df_counts_raw = df_counts_raw.groupby(['path', 'path.1']).size().reset_index(name='frequency')
# df_counts_raw.sort_values(by=["path", "path.1"], inplace=True)


