#import necessary packages
import requests
import numpy as np
import pandas as pd
from dateutil import parser as dateparser
from selectorlib import Extractor
import pg8000
from config import config

#get url for products
url = {"FB_reviews": "https://www.amazon.com/Fitbit-Flex-Black-Version-Count/product-reviews/B01KH2PV4U/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber==%s",
       "BP_reviews": "https://www.amazon.com/Cuisinart-CSBP-100-Stuffed-Burger-Press/product-reviews/B00B58A0OC/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s",
       "AS_reviews": "https://www.amazon.com/OXO-Good-Grips-Avocado-Slicer/product-reviews/B0088LR592/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s",
       "SR_reviews": "https://www.amazon.com/Sweet-Home-Stores-Collection-Contemporary/product-reviews/B00W6819XQ/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s",
       "CM_reviews": "https://www.amazon.com/Elizavecca-Milky-Piggy-Carbonated-Bubble/product-reviews/B00MWI2IS0/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=%s"}

#only take the first 200 pages to gather enough data
num_pages = 200

#using selectorlib to parse url html data
extractor = Extractor.from_yaml_file('selectors.yml')

class WebScrape:

    def scrape(self, url):
        headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/90.0.4430.212 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})
        # Download the page using requests
        #print("Downloading %s"%url)
        r = requests.get(url, headers=headers)
        # Simple check to check if page was blocked (Usually 503)
        if r.status_code > 500:
            if "To discuss automated access to Amazon data please contact" in r.text:
                print("Page %s was blocked by Amazon. Please try using better proxies\n"%url)
            else:
                print("Page %s must have been blocked by Amazon as the status code was %d"%(url,r.status_code))
            return None
        # Pass the HTML of the page and create 
        data = extractor.extract(r.text,base_url=url)
        reviews = []
        columns_to_remove = ["images", "found_helpful", "variant", "verified_purchase"]
        for r in data['reviews']:
            r["product"] = data["product_title"]
            r['url'] = url
            r['rating'] = pd.to_numeric(r['rating'].split(' out of')[0], errors='coerce', downcast='float')
            date_posted = r['date'].split('on ')[-1]
            r['date'] = pd.to_datetime(dateparser.parse(date_posted).strftime('%d %b %Y'))
            [r.pop(key) for key in columns_to_remove]
            reviews.append(r)
        return reviews
    def create_reviews(self):
        """ Connect to the PostgreSQL database server """

        # create sql statement
        create_review_tables = (
                """
                CREATE TABLE IF NOT EXISTS FB_reviews(
                    reviews_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    date DATE NOT NULL,
                    author VARCHAR(255),
                    rating FLOAT NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    url  VARCHAR(255) NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS BP_reviews(
                    reviews_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    date DATE NOT NULL,
                    author VARCHAR(255),
                    rating FLOAT NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    url  VARCHAR(255) NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS AS_reviews(
                    reviews_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    date DATE NOT NULL,
                    author VARCHAR(255),
                    rating FLOAT NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    url  VARCHAR(255) NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS SR_reviews(
                    reviews_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    date DATE NOT NULL,
                    author VARCHAR(255),
                    rating FLOAT NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    url  VARCHAR(255) NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS CM_reviews(
                    reviews_id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    date DATE NOT NULL,
                    author VARCHAR(255),
                    rating FLOAT NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    url  VARCHAR(255) NOT NULL
                )
                """
        )

        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
		
            # create a cursor
            cur = conn.cursor()   

            # execute sql statements
            for command in create_review_tables:
                cur.execute(command)
        
            conn.commit()
        
	    # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def insert_FB_reviews(self, data):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
            # create sql statement
            insert_reviews = (
                """
                INSERT INTO FB_reviews(title, content, date, author, rating, product, url) VALUES(%s, %s, %s, %s, %s, %s, %s)
                """
            )

            # execute sql statements
            for rev in data.values.tolist():
                cur.execute(insert_reviews, rev)
            
            conn.commit()
            
        # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def insert_BP_reviews(self, data):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
            # create sql statement
            insert_reviews = (
                """
                INSERT INTO BP_reviews(title, content, date, author, rating, product, url) VALUES(%s, %s, %s, %s, %s, %s, %s)
                """
            )

            # execute sql statements
            for rev in data.values.tolist():
                cur.execute(insert_reviews, rev)
            
            conn.commit()
            
        # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def insert_AS_reviews(self, data):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
            # create sql statement
            insert_reviews = (
                """
                INSERT INTO AS_reviews(title, content, date, author, rating, product, url) VALUES(%s, %s, %s, %s, %s, %s, %s)
                """
            )

            # execute sql statements
            for rev in data.values.tolist():
                cur.execute(insert_reviews, rev)
            
            conn.commit()
            
        # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            
    def insert_SR_reviews(self, data):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
            # create sql statement
            insert_reviews = (
                """
                INSERT INTO SR_reviews(title, content, date, author, rating, product, url) VALUES(%s, %s, %s, %s, %s, %s, %s)
                """
            )

            # execute sql statements
            for rev in data.values.tolist():
                cur.execute(insert_reviews, rev)
            
            conn.commit()
            
        # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def insert_CM_reviews(self, data):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
            
            # create a cursor
            cur = conn.cursor()
            
            # create sql statement
            insert_reviews = (
                """
                INSERT INTO CM_reviews(title, content, date, author, rating, product, url) VALUES(%s, %s, %s, %s, %s, %s, %s)
                """
            )

            # execute sql statements
            for rev in data.values.tolist():
                cur.execute(insert_reviews, rev)
            
            conn.commit()
            
        # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def update_FB_reviews(self):
        all_reviews = pd.DataFrame(get_reviews(url['FB_reviews'], num_pages))
        sql = """ 
            SELECT date FROM FB_reviews
        """
        conn = None
        try:
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = pg8000.connect(**params)
            # create a new cursor
            cur = conn.cursor()
            # get current table state statement
            cur.execute(sql)
            
            dates = []
            row = cur.fetchone()
            while row is not None:
                dates= dates + row
                row = cur.fetchone()
            dates = pd.to_datetime(pd.Series(dates))

            updated_dates = set(all_reviews['date'].tolist())
            current_dates = set(dates.tolist())
            if updated_dates != current_dates:
                insert_FB_reviews(all_reviews.loc[~all_reviews['date'].isin(list(updated_dates.intersection(current_dates)))])

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def update_BP_reviews(self):
        all_reviews = pd.DataFrame(get_reviews(url['BP_reviews'], num_pages))
        sql = """ 
            SELECT date FROM BP_reviews
        """
        conn = None
        try:
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = pg8000.connect(**params)
            # create a new cursor
            cur = conn.cursor()
            # get current table state statement
            cur.execute(sql)
            
            dates = []
            row = cur.fetchone()
            while row is not None:
                dates= dates + row
                row = cur.fetchone()
            dates = pd.to_datetime(pd.Series(dates))

            updated_dates = set(all_reviews['date'].tolist())
            current_dates = set(dates.tolist())
            if updated_dates != current_dates:
                insert_BP_reviews(all_reviews.loc[~all_reviews['date'].isin(list(updated_dates.intersection(current_dates)))])

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def update_AS_reviews(self):
        all_reviews = pd.DataFrame(get_reviews(url['AS_reviews'], num_pages))
        sql = """ 
            SELECT date FROM AS_reviews
        """
        conn = None
        try:
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = pg8000.connect(**params)
            # create a new cursor
            cur = conn.cursor()
            # get current table state statement
            cur.execute(sql)
            
            dates = []
            row = cur.fetchone()
            while row is not None:
                dates= dates + row
                row = cur.fetchone()
            dates = pd.to_datetime(pd.Series(dates))

            updated_dates = set(all_reviews['date'].tolist())
            current_dates = set(dates.tolist())
            if updated_dates != current_dates:
                insert_AS_reviews(all_reviews.loc[~all_reviews['date'].isin(list(updated_dates.intersection(current_dates)))])

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def update_SR_reviews(self):
        all_reviews = pd.DataFrame(get_reviews(url['SR_reviews'], num_pages))
        sql = """ 
            SELECT date FROM SR_reviews
        """
        conn = None
        try:
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = pg8000.connect(**params)
            # create a new cursor
            cur = conn.cursor()
            # get current table state statement
            cur.execute(sql)
            
            dates = []
            row = cur.fetchone()
            while row is not None:
                dates= dates + row
                row = cur.fetchone()
            dates = pd.to_datetime(pd.Series(dates))

            updated_dates = set(all_reviews['date'].tolist())
            current_dates = set(dates.tolist())
            if updated_dates != current_dates:
                insert_SR_reviews(all_reviews.loc[~all_reviews['date'].isin(list(updated_dates.intersection(current_dates)))])

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def update_CM_reviews(self):
        all_reviews = pd.DataFrame(get_reviews(url['CM_reviews'], num_pages))
        sql = """ 
            SELECT date FROM CM_reviews
        """
        conn = None
        try:
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = pg8000.connect(**params)
            # create a new cursor
            cur = conn.cursor()
            # get current table state statement
            cur.execute(sql)
            
            dates = []
            row = cur.fetchone()
            while row is not None:
                dates= dates + row
                row = cur.fetchone()
            dates = pd.to_datetime(pd.Series(dates))

            updated_dates = set(all_reviews['date'].tolist())
            current_dates = set(dates.tolist())
            if updated_dates != current_dates:
                insert_CM_reviews(all_reviews.loc[~all_reviews['date'].isin(list(updated_dates.intersection(current_dates)))])

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def get_data_from_sql(self, filename):
        """ get data for analysis """
        sql_commands = (""" 
            SELECT * FROM FB_reviews
        """,
        """ 
            SELECT * FROM BP_reviews
        """,
        """ 
            SELECT * FROM AS_reviews
        """,
        """ 
            SELECT * FROM SR_reviews
        """,
        """ 
            SELECT * FROM CM_reviews
        """
        )
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg8000.connect(**params)
		
            # create a cursor
            cur = conn.cursor()   

            # execute sql statements
            data = [["author", "content", "date", "product", "rating", "title", "url"]]
            for command in sql_commands:
                cur.execute(command)

                row = cur.fetchone()
                while row is not None:
                    data.append(row[1:])
                    row = cur.fetchone()
        
            conn.commit()

            write_data = pd.DataFrame(data[1:], columns=data[0])
            write_data.to_csv(filename)
        
	    # close the communication with the PostgreSQL
            cur.close()
        except (Exception, pg8000.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def get_reviews(self, url, num_pages):

        url_pages = []
        for x in range(100, num_pages):
            page = str(x)
            url_pages.append(url % page)

        reviews = []
        for x in range(num_pages-1):
            reviews = reviews + (self.scrape(url_pages[x]))
        return reviews
