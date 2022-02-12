# Amazon Review Sentiment Analysis

In this project, I am looking through amazon reviews of 5 products looking at the sentiment over time. These five products were chosen as they met a the criteria for amount of reviews and were distinct enough to each other to ensure sentiment was independent of market.
First, I scraped the reviews from the first 200 pages for each review. Each reviews is aquired through a special yml file designed for the amazon review page and its details. The reviews are cleaned for any erros and are sent to a google cloud sql server though the python module pg8000 as it is compatible with the postgrSQL server. The reviews are updated weekly through scripts that pull the data from the website and compare the date to check for new entries. These scripts are running on a raspberry pi. The data is then pulled from the server and sentiment analysis is done in R through the tidytext package through three sentiment algorithms. The data from the analysis is then displayed and visualized in tableau. 

Tableau Vizualization avaliable [Here](https://public.tableau.com/app/profile/naolkeb/viz/AMZN_review_sentiment/AmazonReviewSentiment)

![orange-divider](https://user-images.githubusercontent.com/7065401/92672455-187a5f80-f2ef-11ea-890c-40be9474f7b7.png)
