import sys
sys.path.append('../')
import Common.Authentication as auth
import NLPEngine.Engine as nlp
import Common.SqlHandler as sql
from newsapi import NewsApiClient
import MLEngine.Predictor as predictor
import threading
import pandas as pd
import mysql.connector

def process_topic(topic, parameters, candidate):
    top_headlines = newsapi.get_everything(q=parameters)

    for article in top_headlines['articles']:
        article_object = build_article(article)

        article_object['topic'] = topic

        print(article_object['title'])

        content = article_object['content']

        if content is None:
            continue

        score = nlp.sentiment_analyzer_score(article_object['content'])
        sentiment = nlp.parse_vader_score(score, article_object)
        article_object['sentiment'] = sentiment

        main_emotion = nlp.get_emotion(article_object['content'], article_object)
        article_object['emotion'] = main_emotion

        # match article features

        a = pd.DataFrame(article_object, index=[0])
        a_dummy = pd.get_dummies(a)

        political_biasness = predictor.predict(model, a_dummy)

        article_object['political_biasness'] = ''

        save_article(article_object, candidate)


def save_article(article, candidate):
    query = 'INSERT INTO Articles (source, author, title, description, url, url_to_image, published_at, content, sentiment, \
        anger, anticipation, disgust, fear, joy, sadness, surprise, trust, emotion, political_biasness, topic, \
            positive, negative, neutral, candidate) VALUES(%s, %s, %s,%s, %s, %s, %s, %s, %s, \
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                    %s, %s, %s, %s)'

    parameters = (
        article['source'],
        article['author'],
        article['title'],
        article['description'],
        article['url'],
        article['url_to_image'],
        article['published_at'],
        article['content'],
        article['sentiment'],
        article['anger'],
        article['anticipation'],
        article['disgust'],
        article['fear'],
        article['joy'],
        article['sadness'],
        article['surprise'],
        article['trust'],
        article['emotion'],
        article['political_biasness'],
        article['topic'],
        article['vader_pos'],
        article['vader_neg'],
        article['vader_neu'],
        candidate
    )


    sql.execute_non_query(query, parameters, True)


def build_article(article):
    return {
        "source":article['source']['name'],
        "author":article['author'],
        "title":article['title'],
        "description":article['description'],
        "url":article['url'],
        "url_to_image":article['urlToImage'],
        "published_at":article['publishedAt'],
        "content":article['content']
    }

if __name__ == "__main__":
    global model

    db = mysql.connector.connect(
        host        = auth.SQL_HOST_CLOUD,
        user        = auth.SQL_USERNAME_CLOUD,
        password    = auth.SQL_PASSWORD_CLOUD,
        database    = "metrix"
    )

    if(db):
        print('connected')

    newsapi = NewsApiClient(api_key=auth.NEWS_API_API_KEY)
    model = predictor.deseralize()
    nlp.get_lexicon_words()


    cursor = db.cursor()
    cursor.execute('SELECT Category, Parameter FROM Topics WHERE IsEnabled = 1')

    for topic in cursor.fetchall():
        process_topic(topic[0], topic[1], '')

    cursor = db.cursor()
    cursor.execute('SELECT CandidateName, QueryParameters, ID FROM Candidates WHERE IsEnabled = 1')

    for candidate in cursor.fetchall():
        process_topic('Election', candidate[1], candidate[0])

    cursor.close()
    db.close()

    #https://newsapi.org/
  