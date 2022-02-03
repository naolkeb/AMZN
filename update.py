import schedule
import time
import web_scrape

updater = web_scrape.WebScrape()
schedule.every.monday.at("23:45").do(updater.update_FB_reviews)
schedule.every.monday.at("23:45").do(updater.update_BP_reviews)
schedule.every.monday.at("23:45").do(updater.update_AS_reviews)
schedule.every.monday.at("23:45").do(updater.update_SR_reviews)
schedule.every.monday.at("23:45").do(updater.update_CM_reviews)

while True:
    schedule.run_pending
    time.sleep(1)