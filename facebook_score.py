from pymongo import MongoClient
from scipy.spatial import distance
import config

# Script de calcul du score sur la plateforme Facebook
# Connection au cluster :
client = MongoClient(config.MONGO_PASSPHRASE)
db = client[config.MONGO_DB_NAME]
collection = db[config.COLL_PAGES_FB_STATS]
page_fb_collection = db[config.COLL_PAGES_FB]
influence_account = db[config.COLL_INFLUENCE_ACCOUNTS]

# Affectation A une division en fonction du nombre de followers :


division = [0, 1000, 10000, 50000, 500000, 1000000]

for result in collection.find():

    for i in range(len(division)-1):

        if result["page_fans"] >= division[i] and result["page_fans"] < division[i+1]:

            collection.update({"_id": result['_id']}, {
                              "$set": {"division": i+1}})
            influence_account.update_one({"page_fb": result["_id"]}, {
                                         "$set": {"facebook_division": i+1}})

    if result["page_fans"] > division[len(division) - 1]:
        collection.update({"_id": result['_id']}, {
                          "$set": {"division": len(division)}})
        influence_account.update_one({"page_fb": result["_id"]}, {
                                     "$set": {"facebook_division": len(division)}})


# ajout de la metrique page_engagement_rate

for result in collection.find():
    try:
        engaged_users = result["page_engaged_users"]
        nb_fans = result["page_fans"]
        tx = engaged_users / nb_fans
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"page_engagement_rate": tx}})
        influence_account.update_one({"page_fb": result["_id"]}, {
                                     "$set": {"facebook_engagement_rate": round(tx, 2)}})
    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"page_engagement_rate": 0}})
        influence_account.update_one({"page_fb": result["_id"]}, {
                                     "$set": {"facebook_engagement_rate": 0}})
    # ajout de la metrique page_posts_engagement_rate

    try:
        page_engagements = result["page_post_engagements"]
        page_impressions = result["page_posts_impressions"]
        tx = page_engagements / page_impressions
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"page_posts_engagement_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"page_posts_engagement_rate": 0}})

    # ajout de la metrique reach_rate

    try:
        page_impressions_unique = result["page_impressions_unique"]
        nb_fans = result["page_fans"]
        influence_account.update_one({"page_fb": result["_id"]}, {
                                     "$set": {"facebook_reach": page_impressions_unique}})
        influence_account.update_one({"page_fb": result["_id"]}, {
                                     "$set": {"facebook_fans": nb_fans}})
        tx = page_impressions_unique / nb_fans
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"reach_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"reach_rate": 0}})

    # ajout de la metrique fan_adds_rate

    try:
        nb_fans_add = result["page_fan_adds_unique"]
        nb_fans = result["page_fans"]
        tx = nb_fans_add / nb_fans
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"fan_adds_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"fan_adds_rate": 0}})

    # ajout de la metrique fan_removes_rate

    try:
        nb_fans_remove = result["page_fan_removes_unique"]
        nb_fans = result["page_fans"]
        tx = nb_fans_remove / nb_fans
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"fan_removes_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"fan_removes_rate": 0}})

    # ajout de la metrique fan_adds_removes_rate

    try:
        nb_fans_remove = result["page_fan_removes_unique"]
        nb_fans_add = result["page_fan_adds_unique"]
        tx = nb_fans_remove / nb_fans_add
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"fan_adds_removes_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"fan_adds_removes_rate": 0}})
# ajout des metriques sur posts de type video:
    # ajout du taux : vus_unique/vus
    try:
        vus = result["page_video_views"]
        vus_unique = result["page_video_views_unique"]
        tx = vus_unique / vus
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"vu_unique_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"vu_unique_rate": 0}})
    # ajout du taux : vus_click_to_play/vus
    try:
        vus = result["page_video_views"]
        vus_click_to_play = result["page_video_views_click_to_play"]
        tx = vus_click_to_play / vus
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"vu_click_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"vu_click_rate": 0}})

    # ajout du taux : complete_views_30s/views

    try:
        vus = result["page_video_views"]
        vus_complete_30 = result["page_video_complete_views_30s"]
        tx = vus_complete_30 / vus
        collection.update_one({"_id": result['_id']}, {
                              "$set": {"vu_complete_30s_rate": tx}})

    except:

        collection.update_one({"_id": result['_id']}, {
                              "$set": {"vu_complete_30s_rate": 0}})

    # ajout de verification_status

    for doc in page_fb_collection.find():
        # codage de verification_status : 1 indique verified et 0 indique non verified
        if doc["id"] == result["id"]:
            verification_status = 0
            if doc["verification_status"] != "not_verified":
                verification_status = 1

            collection.update_one({"_id": result['_id']}, {
                                  "$set": {"verification_status": verification_status}})

# Normalisation des variables utilisees pour le calcul du score

#### Normalisation des attributs positifs : (x - min) / (max - min)
#### Normalisation des attributs negatifs : (x - max) / (max - min)
division=[1,2,3,4,5,6]
kpis_pos = ["page_fans", "page_engagement_rate",
            "page_posts_engagement_rate", "reach_rate", "fan_adds_rate",
            "fan_adds_removes_rate", "verification_status", "page_video_views", "vu_unique_rate", "vu_click_rate", "vu_complete_30s_rate"]
kpis_neg = ["page_negative_feedback_unique"]
collection = db[config.COLL_PAGES_FB_STATS]
for kpi in kpis_pos:
    for k in kpis_neg:
        for div in division:
            pipeline = [
                {"$match": {"division": div}},
                {"$group": {
                    "_id": "_id",
                    "max": {"$max": "$" + kpi},
                    "min": {"$min": "$" + kpi}
                }
                }

            ]
            pipeline_1 = [
                {"$match": {"division": div}},
                {"$group": {
                    "_id": "_id",
                    "max": {"$max": "$" + k},
                    "min": {"$min": "$" + k}
                }
                }

            ]

            if len(list(collection.aggregate(pipeline))) != 0:

                min = list(collection.aggregate(pipeline))[0]['min']
                max = list(collection.aggregate(pipeline))[0]['max']

            if len(list(collection.aggregate(pipeline_1))) != 0:

                min_1 = list(collection.aggregate(pipeline_1))[0]['min']
                max_1 = list(collection.aggregate(pipeline_1))[0]['max']

                for user in collection.find({"division": div}):

                    if max != min :
                        collection.update_one({"_id": user['_id']},
                                              {"$set": {kpi + "_normal": (user[kpi] - min) / (max - min)}})

                    else:
                        collection.update_one({"_id": user['_id']}, {
                                              "$set": {kpi + "_normal": 0}})
                    if max_1 != min_1 :
                        collection.update_one({"_id": user['_id']},
                                              {"$set": {k + "_normal": (user[k] - max) / (max - min)}})
                    else:
                        collection.update_one({"_id": user['_id']}, {
                                              "$set": {k + "_normal": 0}})


# Implementation de TOPSIS

# Meilleur influenceur : (1, 1, ...) * weights
# Pire influenceur : (0, 0, ...)

kpi_normal = ["page_fans_normal", "page_negative_feedback_unique_normal", "page_engagement_rate_normal",
              "page_posts_engagement_rate_normal", "reach_rate_normal", "fan_adds_rate_normal",
              "fan_adds_removes_rate_normal", "verification_status_normal", "page_video_views_normal", "vu_unique_rate_normal", "vu_click_rate_normal", "vu_complete_30s_rate_normal"]

weights = [0.25, 0.025, 0.15, 0.1, 0.15, 0.05,
           0.025, 0.1, 0.1125, 0.0125, 0.0125, 0.0125]

for inf in collection.find():
    for k in kpi_normal:
        try:
            for w in weights:
                collection.update_one({"_id": inf['_id']}, {
                                      "$set": {k + "_pond": inf[k] * w}})
        except:
            collection.update_one({"_id": inf['_id']}, {
                                  "$set": {k + '_pond': '0'}})

# Meilleur influenceur: (max-min/max-min=1) ou pour negative criteria (max-max/max-min=0)

# best_alt = [1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1 ,1]*weights

best_alt = [0.25, 0, 0.15, 0.1, 0.15, 0.05,
            0.025, 0.1, 0.1125, 0.0125, 0.0125, 0.0125]

# pire influenceur: (min-min/max-min=0)/(min-max/max-min=1)
# best_alt = [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]*weights

worst_alt = [0, 0.025, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


# Calcul du score de Topsis

for inf in collection.find():

    kpi_normal_pond = []

    kpi_normal_pond.append(inf["page_fans_normal_pond"])
    kpi_normal_pond.append(inf["page_negative_feedback_unique_normal_pond"])
    kpi_normal_pond.append(inf["page_engagement_rate_normal_pond"])
    kpi_normal_pond.append(inf["page_posts_engagement_rate_normal_pond"])
    kpi_normal_pond.append(inf["reach_rate_normal_pond"])
    kpi_normal_pond.append(inf["fan_adds_rate_normal_pond"])
    kpi_normal_pond.append(inf["fan_adds_removes_rate_normal_pond"])
    kpi_normal_pond.append(inf["verification_status_normal_pond"])
    kpi_normal_pond.append(inf["page_video_views_normal_pond"])
    kpi_normal_pond.append(inf["vu_unique_rate_normal_pond"])
    kpi_normal_pond.append(inf["vu_click_rate_normal_pond"])
    kpi_normal_pond.append(inf["vu_complete_30s_rate_normal_pond"])


# calcul de la distance euclidienne entre 2 kpis

    s_moins = distance.euclidean(kpi_normal_pond, worst_alt)
    s_plus = distance.euclidean(kpi_normal_pond, best_alt)

    if (s_moins + s_plus) != 0:

        score = (s_moins / (s_moins + s_plus)) * 100

        collection.update_one({"_id": inf['_id']}, {
                              "$set": {"facebook_score": round(score, 2)}})
        influence_account.update_one({"page_fb": inf["_id"]}, {
                                     "$set": {"facebook_score": round(score, 2)}})

    else:
        collection.update_one({"_id": inf['_id']}, {"$set": {"score": 0}})
        influence_account.update_one({"page_fb": inf["_id"]}, {
                                     "$set": {"facebook_score": round(score, 2)}})