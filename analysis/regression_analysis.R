install.packages('lme4')
library(lme4)

df_0 <- read.csv("./treated_pr_logs_0.csv")
df_within_assistment <- read.csv('./treated_pr_logs_0_within_assistments.csv')

model_3 <- glmer(
  next_problem_correctness_binary ~ control_treatment_assignment * scale(prior_5pr_avg_correctness, scale=FALSE) + attempt_count + hint_count + (1|problem_id) + (1|CWA_writer) + (1|class_xid), family = binomial(), data=df_0)
summary(model_3)

model_3_a <- glmer(
  next_problem_correctness_binary ~ control_treatment_assignment * scale(prior_5pr_avg_correctness, scale=FALSE) + attempt_count + hint_count + (1|problem_id) + (1|CWA_writer) + (1|class_xid), family = binomial(), data=df_within_assistment)
summary(model_3_a)


# beyond this is exploratory code so you can ignore it
#
# library(sandwich)
# library(lmtest)
# library(stargazer)
# library(modelsummary)
# library(sjPlot)
# library(sjmisc)
# library(sjlabelled)
# summary(lm(next_problem_correctness_binary ~ control_treatment_assignment, family="binomial", data=df_0))
#
# # mod00=lm(next_problem_correctness_binary~control_treatment_assignment+as.factor(problem_id)+as.factor(student_xid),data=df_0)
#
# model_0 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment + (1|problem_id),
#   family="binomial", data=df_0)
# summary(model_0)
#
# summary(lm(next_problem_correctness_binary ~ control_treatment_assignment * prior_5pr_avg_correctness, family="binomial", data=df_0))
# model_1 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment * scale(prior_5pr_avg_correctness, scale=FALSE)+ (1|problem_id), family = binomial(), data=df_0)
# summary(model_1)
# tab_model(model_1)
#
# model_2 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment * prior_5pr_avg_correctness + attempt_count + hint_count + (1|problem_id) + (1|CWA_writer), family = binomial(), data=df_0)
# summary(model_2)
# tab_model(model_2)
#
#
# model_3 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment * scale(prior_5pr_avg_correctness, scale=FALSE) + attempt_count + hint_count + (1|problem_id) + (1|CWA_writer) + (1|class_xid), family = binomial(), data=df_0)
# summary(model_3)
# tab_model(model_3)
#
#
# df_within_assistment <- read.csv('./treated_pr_logs_0_within_assistments.csv')
# summary(lm(next_problem_correctness_binary ~ control_treatment_assignment, family="binomial", data=df_within_assistment))
# model_0_a <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment + (1|problem_id),
#   family="binomial", data=df_within_assistment)
# summary(model_0_a)
# tab_model(model_0_a)
#
# summary(lm(next_problem_correctness_binary ~ control_treatment_assignment * prior_5pr_avg_correctness , family="binomial", data=df_within_assistment))
# model_1_a <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment * prior_pr10_avg_correctness + (1|problem_id), family = binomial(), data=df_within_assistment)
# summary(model_1_a)
# tab_model(model_1_a)
#
# model_2_a <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment * prior_5pr_avg_correctness + attempt_count + hint_count + (1|problem_id) + (1|CWA_writer), family = binomial(), data=df_within_assistment)
# summary(model_2_a)
# tab_model(model_2_a)
#
# model_3_a <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment * scale(prior_5pr_avg_correctness, scale=FALSE) + attempt_count + hint_count + (1|problem_id) + (1|CWA_writer) + (1|class_xid), family = binomial(), data=df_within_assistment)
# summary(model_3_a)
# tab_model(model_3_a)
#
#
#
#
#
#
#
#
#
#
#
#
# model_0 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment + (1|problem_id),
#   family="binomial", data=df_0)
# summary(model_0)
#
# model_5 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment + (1|problem_id),
#   family="binomial", data=df_5)
# summary(model_5)
#
# model_10 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment + (1|problem_id),
#   family="binomial", data=df_10)
# summary(model_10)
#
#
# model_0_ <- lmer(
#   next_problem_correctness ~ control_treatment_assignment + (1|problem_id),
#   data=df_0)
# summary(model_0_)
#
#
# model_1__ <- lm(next_problem_correctness_binary ~ control_treatment_assignment + factor(problem_id), data = df_0, family="binomial")
# summary(model_1__)
#
#
# model_0_ <- lm(next_problem_correctness_binary ~ control_treatment_assignment, data = df_0, family="binomial")
# summary(model_0_)
# lmtest::bptest(model_0_)
#
# model_0__l <- lm(next_problem_correctness ~ control_treatment_assignment, data = df_0)
# lmtest::bptest(model_0__l)
#
# models.1 <- model_0__l
# models.2 <- coeftest(model_0__l, vcov. = vcovHC(model_0__l, type="HC0"))
# models.3 <- coeftest(model_0__l, vcov. = vcovHC(model_0__l, type="HC1"))
# models.4 <- coeftest(model_0__l, vcov. = vcovHC(model_0__l, type="HC2"))
# models.5 <- coeftest(model_0__l, vcov. = vcovHC(model_0__l, type="HC3"))
#
# stargazer( models.1, models.2, models.3, models.4, models.5,
#   type = "text",
#   digits = 1,
#   header = FALSE,
#   title= "Regression Results")
#
#
#
#
# model_5_ <- lmer(
#   next_problem_correctness ~ control_treatment_assignment + (1|problem_id),
#   data=df_5)
# summary(model_5_, robust=TRUE)
#
# model_10_ <- lmer(
#   next_problem_correctness ~ control_treatment_assignment + (1|problem_id),
#   data=df_10)
# summary(model_10_, robust=TRUE)
#
#
#
#
#
#
#
#
#
# model <- glmer(next_problem_correctness_binary ~ control_treatment_assignment +
#                 prior_pr10_avg_correctness +
#                 (1|problem_id) + (1|student_xid), data=df, family = binomial())
# summary(model)
#
# # model <-  lm(next_problem_correctness_binary ~ control_treatment_assignment + prior_pr10_avg_correctness + factor(student_xid), data=df)
#
# model1 <- glmer(
#   next_problem_correctness_binary ~ control_treatment_assignment + #prior_pr_avg_correctness +
#                                     prior_pr5_avg_correctness +
#                                     (1+control_treatment_assignment|student_xid)
#                                     + (1|problem_id), data=df, family=binomial())
# summary(model1)
#
# # npc~z+pr_id+prior
#
# model <- glm(next_problem_correctness_binary~control_treatment_assignment + factor(CWA_writer) + factor(problem_id), family = "binomial", data = df)
# summary(model)
#
# # npc_binary ~ z + problem_id
#
# model <- glm(next_problem_correctness_binary ~ control_treatment_assignment * prior_pr_avg_correctness + factor(CWA_writer) + factor(problem_id), family = "binomial", data = df)
# summary(model)
#
