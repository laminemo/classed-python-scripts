from pymongo import MongoClient
import pandas as pd
import re
from bson import regex


##### Script de calcul du score sur la plateforme Twitter

#### Connection au cluster :

username = "victor"  ## Identifiant de connection à modifier
mdp = "lataps"

cluster = f"mongodb+srv://{username}:{mdp}@cluster0.bxvcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"


cluster = MongoClient(cluster)


db = cluster["classed"]
influencer_account = db["influencer_account"]


networks = ["facebook, instagram, twitter, youtube, tiktok"]



#### Affectation à une division globale

#### La division globale est choisie en fonction de la plateforme sur laquelle l'influenceur a le plus d'abonnés...


for influencer in influencer_account.find():


    pipeline = [

        {"$group": {
            "_id": "_id",
            "max": {"$max": "$" },
            "min": {"$min": "$" }
        }
        }

    ];


    max_followers = 0
    div = 0

    for network in networks:

        if influencer[network + "_followers"] > max_followers:

            max_followers = influencer[network + "_followers"]
            div = influencer[network + "_division"]


    influencer["division_globale"] = div


    division_globale = 0


    influencer["division_globale"] = division_globale


#### Calcul du score pondéré


#### FORMULE : SOMMME(score_p * followers_p) / SOMME(followers_p)


for influencer in influencer_account.find():

    sum_followers = 0

    score_global = 0


    for network in networks:

        sum_followers = sum_followers + influencer[network + "_followers"]

        score_global = score_global + influencer[network + "_followers"] * influencer[network + "_score"]


    score_global = score_global / sum_followers


    influencer["score_global"] = score_global





