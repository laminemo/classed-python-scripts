import pandas as pd
import json
import matplotlib.pyplot as plt
from pymongo import MongoClient
from mcdm import rank
from datetime import datetime
import numpy as np
from scipy.spatial import distance
import config

# Script de calcul du score sur la plateforme Twitter

# Connection au cluster :
# cluster = f"mongodb+srv://{username}:{mdp}@cluster0.bxvcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"


cluster = MongoClient(config.MONGO_PASSPHRASE)


# Affectation à une division en fonction du nombre de followers :


division_threshold = [1000000, 250000, 50000, 10000, 1500, 500, 10, 0]

division = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

db = cluster["classed"]
collection = db["twitter_accounts"]

twitter_stats = db["twitter_stats2"]
twitter_accounts = db["twitter_accounts"]
influence_accounts = db["influence_account"]


for result in twitter_accounts.find():

    for i in range(len(division_threshold) - 1):

        if result["followers_count"] < division[i+1]:

            twitter_accounts.update_one({"_id": result['_id']}, {
                                        "$set": {"division": i + 1}})
            twitter_stats.update({"author_id": result['twid']}, {
                                 "$set": {"division": i + 1}})

    if result["followers_count"] >= division_threshold[len(division_threshold) - 1]:

        twitter_accounts.update_one({"_id": result['_id']}, {
                                    "$set": {"division": len(division_threshold)}})
        twitter_stats.update_many({"author_id": result['twid']}, {
                                  "$set": {"division": len(division_threshold)}})

    # ajout de la métrique follow_rate :

    twitter_accounts.update_one({"_id": result['_id']}, {"$set": {
                                "follow_rate": result["followers_count"] / result["following_count"]}})


# Ajout de métrique dans les différentes tables (user, stats, media) :

db = cluster["classed"]
collection = db["twitter_stats2"]


for result in collection.find():

    try:
        nb_retweet = result["organic_metrics_retweet_count"]
        nb_like = result["organic_metrics_like_count"]

        impression = result["organic_metrics_impression_count"]

        tx = (nb_retweet + nb_like) / impression

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"engagement_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"engagement_rate": 0}})


# Croissance du compte : gain d'abonnés par rapport au dernier update :

for user in twitter_accounts.find():

    try:
        twitter_accounts.update_one({"_id": user['_id']}, {"$set": {
                                    "follower_list": user["follower_list"] + str(user["followers_count"]) + "_" + user["last_stats"]}})

    except:
        twitter_accounts.update_one({"_id": user['_id']}, {"$set": {
                                    "follower_list": str(user["followers_count"]) + "_" + user["last_stats"]}})


# Création du meilleur et du pire influenceur :

# Si normalisation min-max, pas besoin de calculer le meilleur et le pire, puisqu'ils seront systématiquement :
# (1, 1, 1, ...) et (0, 0, 0, 0...)


# Normalisation des variables utilisées pour le calcul du score

# Normalisation de follow_rate, tweet_count, organic_metrics_user_profile_clicks, engagement_rate, impressions


user_feature_to_normalize = ["follow_rate", "tweet_count"]

#### Normalisation : (x - min) / (max - min)

for feature in user_feature_to_normalize:

    for div in division:

        pipeline = [
            {"$match": {"division": div}},
            {"$group": {
                "_id": "_id",
                "max": {"$max": "$" + feature},
                "min": {"$min": "$" + feature}
            }
            }

        ]

        if len(list(twitter_accounts.aggregate(pipeline))) != 0:

            min = list(twitter_accounts.aggregate(pipeline))[0]['min']
            max = list(twitter_accounts.aggregate(pipeline))[0]['max']

            for user in twitter_accounts.find({"division": div}):

                if (max - min) != 0:

                    twitter_accounts.update_one({"_id": user['_id']},
                                                {"$set": {feature + "_normal": (user[feature] - min) / (max - min)}})

                else:

                    twitter_accounts.update_one({"_id": user['_id']}, {
                                                "$set": {feature + "_normal": 0}})


tweet_feature_to_normalize = ["organic_metrics_user_profile_clicks",
                              "engagement_rate", "organic_metrics_impression_count"]

print("TWEET NORMALIZATION")

for feature in tweet_feature_to_normalize:

    for div in division:

        pipeline = [
            {"$match": {"division": div}},
            {"$group": {
                "_id": "_id",
                "max": {"$max": "$" + feature},
                "min": {"$min": "$" + feature}
            }
            }

        ]

        if len(list(twitter_stats.aggregate(pipeline))) != 0:

            min = list(twitter_stats.aggregate(pipeline))[0]['min']
            max = list(twitter_stats.aggregate(pipeline))[0]['max']

            for tweet in twitter_stats.find({"division": div}):
                if(max - min) != 0:

                    twitter_stats.update_one({"_id": tweet['_id']},
                                             {"$set": {feature + "_normal": (tweet[feature] - min) / (max - min)}})

                else:

                    twitter_stats.update_one({"_id": tweet['_id']}, {
                                             "$set": {feature + "_normal": 0}})


# Calcul du score à partir des KPIs choisies


weights_list = {"follow_rate": 0.1, "engagement_rate": 0.2,
                "impressions": 0.1, "tweet_count": 0.1}

user_collection = db["twitter_accounts"]
media_collection = db["twitter_stats2"]


for user in user_collection.find():

    follow_rate = user["follow_rate"]
    tweet_count = user["tweet_count"]

    verified = False

    if user["verified"] != None:

        verified = True

    av_eng = 0
    av_impression = 0
    av_click = 0

    i = 0

    for media in media_collection.find():

        if media["author_id"] == user["twid"]:

            av_click = av_click + \
                media["organic_metrics_user_profile_clicks_normal"]
            av_eng = av_eng + media["engagement_rate_normal"]
            av_impression = av_impression + \
                media["organic_metrics_impression_count_normal"]

            i = i + 1

            pass

    if i != 0:
        av_eng = av_eng / i
        av_impression = av_impression / i
        av_click = av_click / i

        twitter_accounts.update_one({"_id": user['_id']}, {
                                    "$set": {"av_eng" + "_normal": av_eng}})
        twitter_accounts.update_one({"_id": user['_id']}, {
                                    "$set": {"av_imp" + "_normal": av_impression}})
        twitter_accounts.update_one({"_id": user['_id']}, {
                                    "$set": {"av_click" + "_normal": av_click}})

    else:

        twitter_accounts.update_one({"_id": user['_id']}, {
                                    "$set": {"av_eng" + "_normal": 0}})
        twitter_accounts.update_one({"_id": user['_id']}, {
                                    "$set": {"av_imp" + "_normal": 0}})
        twitter_accounts.update_one({"_id": user['_id']}, {
                                    "$set": {"av_click" + "_normal": 0}})

    score = 0.1 * tweet_count + 10 * av_eng + 0.1 * \
        av_impression + 10 * follow_rate + 0.1 * av_click

    if verified:
        score = score + 5

    user_collection.update_one({"_id": user['_id']}, {
                               "$set": {"score": int(score)}})

    influence_accounts.update_one({"twitter": user["twid"]}, {
                                  "$set": {"twitter_score": int(score)}})


# Implémentation de TOPSIS

# Meilleur influenceur : (1, 1, ...) * weights
# Pire influenceur : (0, 0, ...)

feature_list = ["impressions_normal", "engagement_rate_normal",
                "organic_metrics_user_profile_clicks_normal", "follow_rate_normal", "tweet_count_normal"]

best_i = [0.3, 0.2, 0.2, 0.2, 0.1]
worst_i = [0, 0, 0, 0, 0]


for user in twitter_accounts.find():

    feature_list = []

    feature_list.append(0.3 * user["impressions_normal"])
    feature_list.append(0.2 * user["engagement_rate_normal"])
    feature_list.append(
        0.2 * user["organic_metrics_user_profile_click_normal"])
    feature_list.append(0.2 * user["follow_rate_normal"])
    feature_list.append(0.1 * user["tweet_count_normal"])

    s_moins = distance.euclidean(feature_list, worst_i)
    s_plus = distance.euclidean(feature_list, best_i)

    score = (s_moins / (s_moins + s_plus)) * 100

    user_collection.update_one({"_id": user['_id']}, {
                               "$set": {"topsis_score": int(score)}})

    influence_accounts.update_one({"twitter": user["twid"]}, {
                                  "$set": {"twitter_topsis_score": int(score)}})

    influence_accounts.update_one({"twitter": user["twid"]}, {
                                  "$set": {"twitter_division": user["division"]}})

    influence_accounts.update_one({"twitter": user["twid"]}, {
                                  "$set": {"twitter_followers": user["followers_count"]}})
