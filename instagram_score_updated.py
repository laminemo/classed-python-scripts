## create and configure logger
import logging
logging.basicConfig(filename='try_test1.log', level=logging.DEBUG,
                    format='%(asctime)s:%(message)s',datefmt='%d-%m-%Y:%H:%M:%S',filemode ='w')

logger=logging.getLogger(__name__)
stream_handler =logging.StreamHandler()


#numeric value : Debug : 0 , INFO : 10 ,WARNING : 20 ,ERROR : 30 , CRITICAL : 40
#logging.debug("this is a DEBUG message")
#logging.info("this is an INFO ")
#logging.warning("this is a WARNING")
#logging.error("this is an ERROR")
#logging.critical("this is a heightly CRITICAL")

##Importations des libraires
try:
    from pymongo import MongoClient
    from scipy.spatial import distance
    import config
except:
    logger.exception('!!!Library Not Installed')


##

#numeric value : Debug : 0 , INFO : 10 ,WARNING : 20 ,ERROR : 30 , CRITICAL : 40
#logging.debug("this is a DEBUG message")
#logging.info("this is an INFO ")
#logging.warning("this is a WARNING")
#logging.error("this is an ERROR")
#logging.critical("this is a heightly CRITICAL")



##Connection a mongo
try:
    cluster = MongoClient(config.MONGO_PASSPHRASE)
    base = cluster[config.MONGO_DB_NAME]
    page_ig_stat = base[config.COLL_IG_STATS]
    page_ig = base[config.COLL_IG]
    page_ig_media = base[config.COLL_IG_MEDIA]
    inluence_account = base[config.COLL_INFLUENCE_ACCOUNTS]

except:
    logger.exception('!!!Problem in Connection with Mongo')

##
try:
    base.media.drop()
except Exception as e:
    logging.error("!!!Base media is not Dropded", exc_info=True)


##Division
division = [10000000,5000,2000,1000,500,0]
#division = [0, 50, 100, 500, 5000, 1000000]
try:
    for result in page_ig.find():
        for i in range(len(division)-1):
            if result["followers_count"] <= division[i] and result["followers_count"] > division[i+1]:
                page_ig.update_one({"_id": result['_id']}, {"$set": {"division": i+1}})
        if result["followers_count"] < division[len(division) - 1]:
            page_ig.update_one({"_id": result['_id']}, {"$set": {"division": len(division)}})
except Exception as e:
    logging.error("!!!Division is not ", exc_info=True)

####


##Traitement de la base page_media
try:
    base = cluster[config.MONGO_DB_NAME]
    media = base[config.COLL_IG_MEDIA]
except Exception as e:
    logging.error("!!!Base media not created ", exc_info=True)

##Injection resultat dans la base media
try:
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
except Exception as e:
    logging.error("!!!The aggregation is not injected in base media ", exc_info=True)

##Ajouter le taux d'engagement par post
for result in media.find():
    try:
        media.update_one({"_id": result['_id']}, {"$set": {"Taux_EG_post":  result["engagement"] /result["reach"] }})
    except:
        media.update_one({"_id": result['_id']}, {"$set": {"Taux_EG_post": 0}})

##Inserer le avg_eg dans la page instagram
try:
    agg= media.aggregate(
    [{
    "$group" :
        {"_id" : "$_id",
         "_id" : "$owner",
         "date": { "$max": "$date" },
         "owner": { "$max": "$owner" },
         "nb_posts" : {"$sum" :1},
         "Taux_EG_post" : {"$sum" : "$Taux_EG_post"}
         }}
    ])
    for i in agg:
        print(i)
        try:
            page_ig.update_one({"id": i["owner"]}, {"$set":{'Sum_Eg_Posts':i['Taux_EG_post'] }})
            page_ig.update_one({"id": i["owner"]}, {"$set": {'NB_posts':i['nb_posts']}})
            page_ig.update_one({'_id': i['_id']}, {'$pull': {'owner': None}})
        except:
            page_ig.update_one({"id": i["owner"]}, {"$set": {'Sum_Eg_Posts': 0}})
            page_ig.update_one({"id": i["owner"]}, {"$set": {'NB_posts': 0}})
except Exception as e:
    logging.error("!!!Insert AVG_EG from media into Page_ig is not defined ", exc_info=True)

##Calculer avg_par le nombre de posts ( moyenne des posts )
for res in page_ig.find():
    try:
        Eg_Posts = res["Sum_Eg_Posts"]
        posts = res["NB_posts"]
        avg = Eg_Posts / posts
        page_ig.update_one({"_id": res['_id']}, {"$set": {"AVG_EG_POSTS": avg}})
    except:
        page_ig.update_one({"_id": res['_id']}, {"$set": {"AVG_EG_POSTS": 0}})
##
##Jointure page_ig avec page_stat
try:
    aggre= page_ig_stat.aggregate(
    [{
    "$group" :
        { "_id" : "$_id",
         "_id" : "$id",
         "date": { "$max": "$date" },
         "id": { "$max": "$id" },
         "impressions": {"$sum": "$impressions"},
         "reach": {"$sum": "$reach"},
         "profile_views": {"$sum": "$profile_views"}
         }}
    ])
    for ii in aggre:
        print(ii)
        for result in page_ig.find():
            try:
                page_ig.update_one({"page_id": ii["id"]}, {"$set":{'IMPRESSIONS':ii['impressions'] }})
                page_ig.update_one({"page_id": ii["id"]}, {"$set": {'REACH':ii['reach']}})
                page_ig.update_one({"page_id": ii["id"]}, {"$set": {'profile_VIEWS': ii['profile_views']}})
                page_ig.update_one({"page_id": ii["id"]}, {"$set": {'date': ii['date']}})
                page_ig.update_one({'page_id': ii['id']}, {'$pull': {'id': None}})
            except:
                page_ig.update_one({"id": ii["id"]}, {"$set": {'IMPRESSIONS': 0}})
                page_ig.update_one({"id": ii["id"]}, {"$set": {'REACH': 0}})
                page_ig.update_one({"id": ii["id"]}, {"$set": {'profile_VIEWS': 0}})
except Exception as e:
    logging.error("!!!Problem with Join Page_ig with page_stats", exc_info=True)


##Calcule l'indice de chevauchement
##Calcule le reach_rate
try:
    ##Calcule le reach_rate
    for result in page_ig.find():
        try:
            followers = result["followers_count"]
            reach = result["REACH"]
            reach_rate = reach / followers
            follows=result["follows_count"]
            Follow_rate = followers / follows
            page_ig.update_one({"_id": result['_id']}, {"$set": {"REACH_RATE": reach_rate}})
            page_ig.update_one({"_id": result['_id']}, {"$set": {"FOLLOW_RATE": Follow_rate}})
        except:
            page_ig.update_one({"_id": result['_id']}, {"$set": {"REACH_RATE": 0}})
            page_ig.update_one({"_id": result['_id']}, {"$set": {"FOLLOW_RATE": 0}})
except Exception as e:
    logging.error("!!!ERROR in calculation for KPIS", exc_info=True)


## Normamlization
try:
    division_div =[1,2,3,4,5]
    instagram_features = [ "followers_count", "FOLLOW_RATE" ,"REACH_RATE" , "AVG_EG_POSTS", "profile_VIEWS"]
    for feature in instagram_features:
        print(feature)
        for div in division_div:
            print(div)
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
        for i in res:
            print(i)
            if len(list(page_ig.aggregate(pipeline))) != 0:
                min = list(page_ig.aggregate(pipeline))[0]['min']
                print(min)
                max = list(page_ig.aggregate(pipeline))[0]['max']
                print(max)
                for user in page_ig.find({"division": div}):
                    if (max - min) != 0:
                        page_ig.update_one({"_id": user['_id']},{"$set": {feature + "_normal": (user[feature] - min) / (max - min)}})
                    #else:
                        #page_ig.update_one({"_id": user['_id']}, {"$set": {feature + "_normal": 0}})
except Exception as e:
    logging.error("!!!ERROR Normamlization", exc_info=True)

#### DROP BASE MEDIA
try:
    base.media.drop()
except Exception as e:
    logging.error("!!!ERROR media Not Dropped", exc_info=True)

#### Implémentation de TOPSIS
try:
    feature_list = [ "followers_count_normal", "FOLLOW_RATE_normal" ,"REACH_RATE_normal" , "AVG_EG_POSTS_normal", "profile_VIEWS_normal"]
    best_i = [0.35,0.05,0.2,0.3,0.1]
    worst_i = [0, 0, 0, 0, 0]
    for user in page_ig.find():
        feature_list = []

        feature_list.append(0.35 * user["followers_count_normal"])
        feature_list.append(0.05 * user["FOLLOW_RATE_normal"])
        feature_list.append(0.2 * user["REACH_RATE_normal"])
        feature_list.append(0.3 * user["AVG_EG_POSTS_normal"])
        feature_list.append(0.1 * user["profile_VIEWS_normal"])

        s_moins = distance.euclidean(feature_list, worst_i)
        s_plus = distance.euclidean(feature_list, best_i)

        score = (s_moins / (s_moins + s_plus)) * 100

        page_ig.update_one({"_id": user['_id']}, {"$set": {"S C O R E ": int(score) }})
        inluence_account.update_one({"instagram": user["id"]}, {"$set": {"instagram_score": int(score)}})
        inluence_account.update_one({"instagram": user["id"]}, {"$set": {"instagram_division": user["division"]}})
        inluence_account.update_one({"instagram": user["id"]}, {"$set": {"instagram_followers": user["followers_count"]}})
except Exception as e:
    logging.error("!!!ERROR CALCULTAE TOPSIS", exc_info=True)


