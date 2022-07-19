import pandas as pd
import re as regex
import instaloader
import requests
import json
import os
from datetime import date
import boto3
from io import StringIO
from dotenv import load_dotenv
load_dotenv()
import time
import datetime
from datetime import timedelta


class InstagramScraper():
    def __init__(self):
        """
        Note: 
            2. When using this scraper, Instagram Username and Password need to be declared
        Purpose: This scraper parse the data from Instagram using video url, and upload the json format result to S3 bucket
        Parsing data includes:
            1. Video Engagement Data: 
                view_count
                like_count
                comment_count
            2. Video Creator Data:
                media_count
                follwer_count
                follwee_count
                link_in_bio
                creator_biography
            3. Vido Data:
                caption
                hashtag
                mention
                video_duration
                creator_username
                creator_id
                date_utc
                date_local
                video_typename
                video_location
        """
        # define header
        self.headers = {
            'User-Agent' : os.getenv("USER_AGENT"),}
        
        #import video url
        self.api_url = os.getenv("API_URL")
        self.api_key = os.getenv("API_KEY")
        self.urls = self.api_url + "posts/?key=" + self.api_key
        self.data = requests.get(url=self.urls)
        self.data.encoding = 'utf-8'
        self.data = json.loads(self.data.text)
        self.links = pd.DataFrame.from_dict(self.data)
        self.links = self.links.loc[self.links["active"]==True]
        self.links.reset_index(inplace=True)
        
        #generate shortcode using url
        self.links["short_code"] = self.links["url"].apply(self.GetShortCode)
        
        #generate media id using shortcode
        self.links["media_id"] = self.links["short_code"].apply(self.CodeToMediaId)
        
        #scraper API declearation
        self.L = instaloader.Instaloader()
        self.USER = os.getenv("IG_USERNAME")
        self.PASSWORD = os.getenv("IG_PASSWORD")
        self.L.login(self.USER, self.PASSWORD)
    
    def GetShortCode(self, url):
        """
        Perpose: get mdeia shortcode based on video url
        Input: url
        Output: a string shortcode
        Reference: https://regex101.com/library/ZUHk60
        """
        try:
            expression = '/(?:https?:\/\/)?(?:www.)?instagram.com\/?([a-zA-Z0-9\.\_\-]+)?\/([p]+)?([reel]+)?([tv]+)?([stories]+)?\/([a-zA-Z0-9\-\_\.]+)\/?([0-9]+)?'
            short_code = regex.search(expression, url).group(6)
            return short_code
        except:
            print("Failed to Extract Short Code form URL")
            return 
    
    def CodeToMediaId(self, short_code):
        """
        Perpose: get mdeia id based on video shortcode
        Input: string video short code
        Output: midedia id
        """
        try:
            alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
            media_id = 0
            for letter in short_code:
                media_id = (media_id*64) + alphabet.index(letter)

            return media_id
        except:
            print("Failed to convert short code to media id")
            return
    
    def getPostInfo(self, media_id):
        """
        Perpose: scrape video post information using media_id
        Input: midea id
        Output: a dictionary contains view_count, like_count, comment_count, caption,
                hashtag, mention, video_duration, creator_username, creator_id, date_utc,
                date_local, video_typename, video_location
        """
        #store data in dictionary
        info_container = {}
        try:
            post = instaloader.Post.from_mediaid(self.L.context, int(media_id))
            #engagement infprofile.followees
            info_container["video_view_count"] = post.video_view_count
            info_container["video_like_count"] = post.likes
            info_container["video_comment_count"] = post.comments

            #video info
            info_container["video_caption"] = post.caption
            info_container["video_hashtag"] = post.caption_hashtags
            info_container["video_mention"] = post.caption_mentions
            info_container["video_duration"] = post.video_duration
            info_container["creator_username"] = post.owner_username
            info_container["creator_id"] = post.owner_id
            info_container["video_create_date_utc"] = post.date_utc.strftime("%Y/%m/%d")
            info_container["video_create_date_local"] = post.date_local.strftime("%Y/%m/%d")
            info_container["video_typename"] = post.typename
            info_container["video_location"] = post.location
            info_container["post_EM"] = ""
            return info_container
        except Exception as e:
            if str(e) == "Fetching Post metadata failed.":
                info_container["post_EM"] = "DNE"
            else:
                info_container["post_EM"] = "PostEM: " + str(e)
            return info_container

    
    def getProfileInfo(self, creator_username):
        """
        Perpose: scrape video creator profile information using creator username
        Input: creator username
        Output: a dictionary contains media_count, follwer_count, follwee_count,
                link_in_bio, creator_biography
        """
        #store data in dictionary
        info_container = {}
        try:
            profile = instaloader.Profile.from_username(self.L.context, creator_username)
            info_container["creator_media_count"] = profile.mediacount
            info_container["creator_follwer_count"] = profile.followers
            info_container["creator_follwee_count"] = profile.followees
            info_container["link_in_bio"] = profile.external_url
            info_container["creator_biography"] = profile.biography
            info_container["profile_EM"] = ""
            return info_container
        except Exception as e:
            info_container["profile_EM"] = "ProfileEM: " + str(e)
            return info_container
    
    
    def getComments(self, media_id):
        """
        Perpose: scrape video comments information using video media id
        Input: video media id
        Output: a dictionary with keys comment_id, comment_owner_username,
                comment_text, comment_create_date; value of a key is a list
                of data
        """
        comment_info_dict = {}
        try:
            post = instaloader.Post.from_mediaid(self.L.context, int(media_id))
            comment_info_dict = {
                "comment_id": [],
                "comment_owner_username": [],
                "comment_text": [],
                "comment_create_date": []}
            for comment in post.get_comments():
                comment_info_dict["comment_id"].append(str(comment.id))
                comment_info_dict["comment_owner_username"].append(comment.owner.username)
                comment_info_dict["comment_text"].append(comment.text)
                comment_info_dict["comment_create_date"].append(str(comment.created_at_utc))
                comment_info_dict["comment_EM"] = ""
            return comment_info_dict
        except Exception as e:
            if str(e) == "Fetching Post metadata failed.":
                comment_info_dict["comment_EM"] = "DNE"
            else:
                comment_info_dict["comment_EM"] = "CommentEM: " + str(e)
            return comment_info_dict
    
    def deactiveURL(self, apiURL):
        """
        Purpose: change the DNE url's active status to False in database
        Input: unique url of the video in database
        Output: None
        """
        deactivate_info = {"active": False}
        requests.patch(url=apiURL, json=deactivate_info)

    def uploadFile(self, result_dataframe):
        """
        Perpose: upload the result dataframe to s3 bucket in json format
        Input: data frame
        Output: None
        """
        try:
            cur_date_timestamp = int(time.mktime(date.today().timetuple()))
            cur_time_timestamp = int(time.mktime(datetime.datetime.now().timetuple()))
            file_name = (os.getenv("PLATFORM_NAME") + 
                        str(cur_date_timestamp) + 
                        "/" + 
                        str(cur_time_timestamp) + 
                        "_output.json")
            bucket = os.getenv("BUCKET_NAME") # already created on S3
            json_buffer = StringIO()
            result_dataframe.to_json(json_buffer)
            s3_resource = boto3.resource('s3',
                                        endpoint_url=os.getenv("ENDPOINT_URL"),
                                        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                                        aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"))

            s3_resource.Object(bucket, file_name).put(Body=json_buffer.getvalue())
        except Exception as e:
            print("Failed to Upload File to S3 Due to:", str(e))
            return e

    def generateDataFrame(self):
        """
        Purpose: generate a dataframe and csv file that contains all info we need
        Input: export file name
        Output: a datafram and a csv file contians engagement, creator, video related features
        """
        try:
            self.links["post_Info"] = self.links["media_id"].apply(self.getPostInfo)
            post_df = pd.json_normalize(self.links["post_Info"])

            self.links["profile_Info"] = post_df["creator_username"].apply(self.getProfileInfo)
            profile_df = pd.json_normalize(self.links["profile_Info"])

            self.links["comment_info"] = self.links["media_id"].apply(self.getComments)
            comment_df = pd.json_normalize(self.links["comment_info"])

            result = pd.concat([post_df, profile_df, comment_df], axis=1)
            result["media_id"] = self.links["media_id"].copy()
            result["short_code"] = self.links["short_code"].copy()
            result["url"] = self.links["url"].copy()
            
            #get local url for later status change if DNE detected
            result["unique_id"] = self.links["id"]
            result["local_url"] = self.api_url + "posts/" + result["unique_id"] + "?key=" + self.api_key
            
            result["scraper_running_timestamp"] = datetime.datetime.now().isoformat()
            
            #error handling
            result.loc[result['post_EM'] == "DNE", "Error_Message"] = "DNE"
            result.loc[result['post_EM'] != "DNE", "Error_Message"] = (result['post_EM'] + "&" 
                                                                       + result['profile_EM'] + "&" 
                                                                       + result["comment_EM"])
            result["Error_Message"] = result["Error_Message"].apply(lambda x: set(x.split("&")))
            result["Status"] = result["Error_Message"].apply(lambda x: "ERROR" if x!={""} else "OK")
            
            #deactive the url if it does not exist
            DNE_URL = result.loc[result["Error_Message"]=={"DNE"}]
            DNE_URL["local_url"].apply(self.deactiveURL)
            
            #upload result to s3
            self.uploadFile(result)
            return result
        except Exception as e:
            return str(e)


if __name__=="__main__":
    start = time.time()
    scraper = InstagramScraper()
    scraper.generateDataFrame()
    end = time.time()

    # timer
    total_second = end - start
    minute = total_second // 60
    second = round(total_second % 60, 2)
    print("Scraper spent {} minutes {} seconds to extract data.".format(minute, second))

            
        