###############importations des libraires
from pymongo import MongoClient
from pprint import pprint
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas.io.json import json_normalize
import json

################## connection a mongo #####################
cluster = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
base=cluster.page_insta
names=base.list_collection_names()
database = cluster['page_insta']

############## creation des tables ####################


page_ig_stat = base['page_ig_stats']
page_ig = base['page_ig']
page_ig_media = base['page_ig_media']
inluence_account = base['influence_accounts ']

#############################
database.media2.drop()
database.media.drop()

############### division ############
division = [0, 100, 200, 50000, 50000, 1000000]
for result in page_ig.find():
        for i in range(len(division)-1):
            print(division[i])
            if result["followers_count"] >= division[i] and result["followers_count"] < division[i+1] :
                page_ig.update({"_id": result['_id']}, {"$set": {"division": i+1}})
        if result["followers_count"] > division[len(division) - 1]:
                page_ig.update({"_id": result['_id']}, {"$set": {"division": len(division)}})

################ jointure page_ig avec page_stat
for result in page_ig.find():
    for doc in page_ig_stat.find():
        ### codage de verification_status : 1 indique verified et 0 indque non verified
        if doc["id"] == result["page_id"]:
            ##print(max(doc['date'])
            ##if doc['date']==max(doc['date']):
                reach=doc["reach"]
                impressions=doc['impressions']
                profile_views=doc['profile_views']
    page_ig.update_one({"_id": result['_id']}, {"$set": {"reach": reach,"impressions":impressions,"profile_views":profile_views}})
####### calcule l'indice de chevauchement ###########"
    page_ig.update_one({"_id": result['_id']},
                           {"$set": {"follow_rate": result["followers_count"] / result["follows_count"]}})

############### calcule le reach_rate ##############
for result in page_ig.find():
    for doc in page_ig_stat.find():
        ### codage de verification_status : 1 indique verified et 0 indque non verified
        if doc["id"] == result["page_id"]:
            try:
                followers = page_ig["followers_count"]
                reach = page_ig_stat["reach"]
                tx = reach / followers
                page_ig.update_one({"_id": result['_id']}, {"$set": {"reach_rate": tx}})
            except:
                page_ig.update_one({"_id": result['_id']}, {"$set": {"reach_rate": 0}})

#################  creation  la base media
database.media2.drop() ## s'il exist
database=cluster['page_insta']
media = database["media"]

###################################### injecter le resultat dans la base media
agg_result = page_ig_media.aggregate(
    [{
        "$group":
            {
                "_id": "$id",
                "date": {"$max": "$date"},
                "owner": {"$max": "$owner"},
                "impressions": {"$sum": "$impressions"},
                "reach": {"$sum": "$reach"},
                "engagement": {"$sum": "$engagement"}

            }}
    ])
for i in agg_result:
    media.insert_many(agg_result, ordered=False)
######## ajouter le taux d'engagement par avg_posts
for result in media.find():
    try:
        rea=result["reach"]
        eg=result["engagement"]
        tx = eg / rea
        media.update_one({"_id": result['_id']}, {"$set": {"taux_EG": tx}})
    except:
        media.update_one({"_id": result['_id']}, {"$set": {"taux_EG": 0}})
####### create Database 2

database=cluster['page_insta']
media2 = database["media2"]
########## inserer dans database 2
agg= media.aggregate(
    [{
    "$group" :
        {"_id" : "$id",
         "_id" : "$owner",
         "date": { "$max": "$date" },
         "owner": { "$max": "$owner" },
            "_id" : "$owner",
         "nb_posts" : {"$sum" :1},
         "impressions" : {"$sum" : "$impressions"},
         "reach" : {"$sum" : "$reach"},
         "engagement" : {"$sum" : "$engagement"},
         "taux_EG" : {"$sum" : "$taux_EG"}
         }}
    ])
for i in agg:
    media2.insert_many(agg, ordered=True)
############ supprimer la base 1
database.media.drop()
#################### calculer avg_par le nombre de posts ( moyenne des posts )
for res in media2.find():
    try:
        impr = res["impressions"]
        nb_po = res["nb_posts"]
        rea = res["reach"]
        eg = res["engagement"]
        tx = res["taux_EG"]
        impression = impr / nb_po
        reach = rea / nb_po
        egage = eg / nb_po
        tay = tx / nb_po
        media2.update_one({"_id": res['_id']}, {"$set": {"IMPR": impression}}),
        media2.update_one({"_id": res['_id']}, {"$set": {"REA": reach}}),
        media2.update_one({"_id": res['_id']}, {"$set": {"EG": egage}}),
        media2.update_one({"_id": res['_id']}, {"$set": {"TX_EG": tay}}),
    except:
        media2.update_one({"_id": res['_id']}, {"$set": {"REA": 0}})
        media2.update_one({"_id": res['_id']}, {"$set": {"IMPR": 0}})
        media2.update_one({"_id": res['_id']}, {"$set": {"EG": 0}})
        media2.update_one({"_id": res['_id']}, {"$set": {"TX_EG": 0}})

##### jointure page_ig avec page_media2

for result in page_ig.find():
    for doc in media2.find():
        ##codage de verification_status : 1 indique verified et 0 indque non verified
        #print(doc["owner"] == result["id"])
        if doc["owner"] == result["id"]:
            IMPR =doc["IMPR"]
            TX_EG =doc['TX_EG']
            page_ig.update_one({"_id": result['_id']}, {"$set": {"IMPR": IMPR}})
            page_ig.update_one({"_id": result['_id']}, {"$set": {"TX_EG": TX_EG}})
        else:
            page_ig.update_one({"_id": result['_id']}, {"$set": {"IMPR": 0}})
            page_ig.update_one({"_id": result['_id']}, {"$set": {"TX_EG": 0}})

########################### Normamlization
division =[1,2,3,4,5,6]
instagram_features = ["follow_rate", "followers_count", "reach_rate","IMPR", "TX_EG","impressions", "reach", "profile_views"]
for feature in instagram_features:
    for div in division:
        pipeline = [
            {"$match": {"division": div}},
            {"$group": {
                "_id": "_id",
                "max": {"$max": "$" + feature},
                "min": {"$min": "$" + feature}
            }
            }
        ];
        res = page_ig.aggregate(pipeline)
        if len(list(page_ig.aggregate(pipeline))) != 0:
            min = list(page_ig.aggregate(pipeline))[0]['min']
            max = list(page_ig.aggregate(pipeline))[0]['max']
            for user in page_ig.find({"division": div}):
                if (max - min) != 0:
                    page_ig.update_one({"_id": user['_id']},
                                       {"$set": {feature + "_normal": (user[feature] - min) / (max - min)}})
                else:
                    page_ig.update_one({"_id": user['_id']}, {"$set": {feature + "_normal": 0}})

####################### ponderation
feature_normal = ["impressions_normal", "reach_normal", "profile_views_normal", "follow_rate_normal", "followers_count_normal", "IMPR_normal", "TX_EG_normal",
                  "reach_rate_normal"]
weights = [0.1, 0.2, 0.15, 0.05, 0.2, 0.05, 0.2, 0.05]
for inf in page_ig.find():
    for k in feature_normal:
        try:
            for w in weights:
                page_ig.update_one({"_id": inf['_id']}, {"$set": {k + "_pond": inf[k] * w}})
        except:
            page_ig.update_one({"_id": inf['_id']}, {"$set": {k + '_pond': '0'}})

############################## Topsis
from scipy.spatial import distance
#feature_normal = ["impressions_normal", "reach_normal", "profile_views_normal", "follow_rate_normal", "followers_count_normal

best_alt = [0.1, 0.2, 0.15, 0.05, 0.2, 0.05, 0.2, 0.05]
worst_alt = [0, 0, 0, 0, 0, 0, 0, 0]
## euclidean_distance
##Calcul du score de Topsis

for inf in page_ig.find():

    feature_normal_pond = []

    feature_normal_pond.append(inf["impressions_normal_pond"])
    feature_normal_pond.append(inf["reach_normal_pond"])
    feature_normal_pond.append(inf["profile_views_normal_pond"])
    feature_normal_pond.append(inf["follow_rate_normal_pond"])
    feature_normal_pond.append(inf["followers_count_normal_pond"])
    feature_normal_pond.append(inf["IMPR_normal_pond"])
    feature_normal_pond.append(inf["TX_EG_normal_pond"])
    feature_normal_pond.append(inf["reach_rate_normal_pond"])

    s_moins = distance.euclidean(feature_normal_pond, worst_alt)
    s_plus = distance.euclidean(feature_normal_pond, best_alt)

    if (s_moins + s_plus) != 0:

        score = (s_moins / (s_moins + s_plus)) * 100

        page_ig.update_one({"_id": inf['_id']}, {"$set": {"score": score}})

    else:
        page_ig.update_one({"_id": inf['_id']}, {"$set": {"score": 0}})
        inluence_account.update_one({"page_id": user["id"]}, {"$set": {"instagram_score": int(score)}})
