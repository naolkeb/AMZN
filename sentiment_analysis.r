rm(list = ls())

library(tidytext)
library("tidyverse")

get_sentiments("afinn")

get_sentiments("bing")

get_sentiments("nrc")

library('dplyr')
library("stringr")

reviews = read.csv("AMZN_Reviews.csv", header=T)
View(reviews)

tokenized_reviews <- tibble(rev_num = reviews[,1], text = reviews[,3]) %>% 
  unnest_tokens(word, text)

by_rev <- tokenized_reviews %>% group_by(rev_num)

bing_sentiment <- get_sentiments("bing")
afinn_sentiment <- get_sentiments("afinn")
nrc_sentiment <- get_sentiments("nrc")

by_rev_bing <- by_rev %>%
  inner_join(bing_sentiment)

ratio <- full_join(by_rev_bing %>% 
  filter(sentiment == "positive") %>%
  count(), by_rev_bing %>% 
  filter(sentiment == "negative") %>%
  count(), by="rev_num", suffix=c("_pos","_neg")) %>%
  replace(is.na(.), 0)
View(ratio)

rat <- ratio[,2] / ratio[,3]

rat <- lapply(rat[,1], function(i) if(is.numeric(i)) ifelse(is.infinite(i), 10, i) else i)

rat <- data.frame(matrix(unlist(rat), nrow=length(rat), byrow=TRUE))
colnames(rat) <- c("bing_ratio")

ratio <- cbind(ratio, rat)

View(ratio)

reviews <- full_join(reviews, ratio, by= c("X" = "rev_num")) %>%
  replace(is.na(.), -1)
View(reviews)

by_rev_afinn <- by_rev %>%
  inner_join(afinn_sentiment)

by_rev_afinn <- by_rev_afinn %>%
  group_by(rev_num)
by_rev_afinn <- aggregate(by_rev_afinn$value, by= list(rev_num = by_rev_afinn$rev_num), FUN=sum)

View(by_rev_afinn)
colnames(by_rev_afinn) <- c("rev_num","afinn_sentiment")

reviews <- full_join(reviews, by_rev_afinn, by= c("X" = "rev_num")) %>%
  replace(is.na(.), -1)
View(reviews)

tokenized_reviews <- tibble(product_review = reviews[,5], text = reviews[,3]) %>% 
  unnest_tokens(word, text)
by_product <- tokenized_reviews %>% group_by(product_review)
nrc_analysis <- by_product %>%
  count(word)
nrc_analysis <- nrc_analysis[order(nrc_analysis$n, decreasing = TRUE),]
data(stop_words)
nrc_analysis <- nrc_analysis %>%
  anti_join(stop_words) %>%
  inner_join(nrc_sentiment)
View(nrc_analysis)

library(ggplot2)
reviews$date <- as.Date(reviews$date)
reviews_products <- unique(reviews$product)
View(reviews_products)


ggplot(reviews %>%
    filter(afinn_sentiment != -1) %>%
    group_by(month = lubridate::floor_date(date, "month")) %>%
    summarize(sentiment = mean(afinn_sentiment)), aes(x = month,y = sentiment)) + 
    geom_point(size= 1, shape= 2)

ggplot(reviews %>%
    filter(bing_ratio != -1) %>%
    group_by(month = lubridate::floor_date(date, "month")) %>%
    summarize(sentiment = mean(bing_ratio)), aes(x = month,y = sentiment)) + 
    geom_point(size= 1, shape= 2)

write.csv(reviews, "sentiment.csv", row.names = FALSE)
