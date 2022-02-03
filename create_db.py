import web_scrape
import pandas as pd

db_creator = web_scrape.WebScrape()

#get url for products
url = {"FB_reviews": "https://www.amazon.com/Fitbit-Flex-Black-Version-Count/product-reviews/B01KH2PV4U/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber==%s",
       "BP_reviews": "https://www.amazon.com/Cuisinart-CSBP-100-Stuffed-Burger-Press/product-reviews/B00B58A0OC/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s",
       "AS_reviews": "https://www.amazon.com/OXO-Good-Grips-Avocado-Slicer/product-reviews/B0088LR592/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s",
       "SR_reviews": "https://www.amazon.com/Sweet-Home-Stores-Collection-Contemporary/product-reviews/B00W6819XQ/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s",
       "CM_reviews": "https://www.amazon.com/Elizavecca-Milky-Piggy-Carbonated-Bubble/product-reviews/B00MWI2IS0/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s"}

#only take the first 200 pages to gather enough data
num_pages = 200

FR_reviews = pd.DataFrame(db_creator.get_reviews(url['FB_reviews'], num_pages))
HP_reviews = pd.DataFrame(db_creator.get_reviews(url['BP_reviews'], num_pages))
CAH_reviews = pd.DataFrame(db_creator.get_reviews(url['AS_reviews'], num_pages))
SP_reviews = pd.DataFrame(db_creator.get_reviews(url['SR_reviews'], num_pages))
BS_reviews = pd.DataFrame(db_creator.get_reviews(url['CM_reviews'], num_pages))

db_creator.create_reviews()

db_creator.insert_FR_reviews(FR_reviews)
db_creator.insert_HP_reviews(HP_reviews)
db_creator.insert_CAH_reviews(CAH_reviews)
db_creator.insert_SP_reviews(SP_reviews)
db_creator.insert_BS_reviews(BS_reviews)


