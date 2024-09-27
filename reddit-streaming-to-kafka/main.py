#TODO: add logs and error handling
from json import dumps
import time
from kafka import KafkaProducer
import configparser
import praw
import threading

threads = []
top_subreddit_list = ['AskReddit', 'funny', 'gaming', 'aww', 'worldnews']
countries_subreddit_list = ['india', 'usa', 'unitedkingdom', 'australia']


class RedditProducer:

    def __init__(self, subreddit_list: list[str], cred_file: str="secrets/credentials.cfg"):

        self.subreddit_list = subreddit_list
        self.reddit = self.__get_reddit_client__(cred_file)
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x:
                            dumps(x).encode('utf-8'),
                            acks='all'
                            
                        )


    def __get_reddit_client__(self, cred_file) -> praw.Reddit:

        config = configparser.ConfigParser()
        config.read_file(open(cred_file))

        try:
            client_id: str = config.get("reddit", "client_id")
            client_secret: str = config.get("reddit", "client_secret")
            user_agent: str = config.get("reddit", "user_agent")
        except configparser.NoSectionError:
            raise ValueError("The config file does not contain a reddit credential section.")
        except configparser.NoOptionError as e:
            raise ValueError(f"The config file is missing the option {e}")
        
        return praw.Reddit(
            user_agent = user_agent,
            client_id = client_id,
            client_secret = client_secret
        )


    def start_stream(self, subreddit_name) -> None:
        subreddit = self.reddit.subreddit(subreddit_name)
        comment: praw.models.Comment
        for comment in subreddit.stream.comments(skip_existing=True):
            try:
                comment_json: dict[str, str] = {
                    "id": comment.id,
                    "name": comment.name,
                    "author": comment.author.name,
                    "body": comment.body,
                    "subreddit": comment.subreddit.display_name,
                    "upvotes": comment.ups,
                    "downvotes": comment.downs,
                    "over_18": comment.over_18,
                    "timestamp": comment.created_utc,
                    "permalink": comment.permalink,
                }

                self.producer.send("reddit_comments",value=comment_json, key=comment.id.encode("utf-8"))
                print(f"subreddit: {subreddit_name}, comment: {comment_json} \n")
                time.sleep(10)
            except Exception as e:
                print("An error occurred:", str(e))
    
    def start_streaming_threads(self):
        for subreddit_name in self.subreddit_list:
            thread = threading.Thread(target=self.start_stream, args=(subreddit_name,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    reddit_producer = RedditProducer(top_subreddit_list + countries_subreddit_list)
    reddit_producer.start_streaming_threads()
