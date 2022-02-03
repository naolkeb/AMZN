import web_scrape

data_retriever = web_scrape.WebScrape()

data_retriever.get_data_from_sql("AMZN_Reviews.csv")