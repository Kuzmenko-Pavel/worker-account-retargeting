#include <vector>
#include <boost/algorithm/string.hpp>
#include "../config.h"
#include <mongocxx/instance.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/read_preference.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/options/find.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/value.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>

#include "ParentDB.h"
#include "Log.h"
#include <KompexSQLiteStatement.h>
#include "json.h"
#include "Config.h"
#include "Offer.h"

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using mongocxx::options::find;
using mongocxx::read_preference;

mongocxx::instance instance{};

ParentDB::ParentDB()
{
    pdb = Config::Instance()->pDb->pDatabase;
}

ParentDB::~ParentDB()
{
    //dtor
}

void ParentDB::loadRating(const std::string &id)
{
    int c = 0;
    //std::unique_ptr<mongo::DBClientCursor> cursor;
    //std::vector<mongo::BSONObj> bsonobjects;
    //std::vector<mongo::BSONObj>::const_iterator x;
    //Kompex::SQLiteStatement *pStmt;
    //pStmt = new Kompex::SQLiteStatement(pdb);
    //mongo::BSONObj f = BSON("adv_int"<<1<<"guid_int"<<1<<"full_rating"<<1);
    //mongo::Query query;
    //if(!id.size())
    //{
    //            query = mongo::Query("{\"full_rating\": {\"$exists\": true}}");
    //}
    //else
    //{
    //            query =  mongo::Query("{\"adv_int\":"+ id +", \"full_rating\": {\"$exists\": true}}");

    //    bzero(buf,sizeof(buf));
    //    sqlite3_snprintf(sizeof(buf),buf,"DELETE FROM Informer2OfferRating WHERE id_inf=%s;",id.c_str());
    //    try
    //    {
    //        pStmt->SqlStatement(buf);
    //    }
    //    catch(Kompex::SQLiteException &ex)
    //    {
    //        logDb(ex);
    //    }
    //    pStmt->FreeQuery();
    //}
    //
    //try{
    //    cursor = monga_main->query(cfg->mongo_main_db_ + ".stats_daily.rating", query, 0,0, &f);
    //    unsigned int transCount = 0;
    //    pStmt->BeginTransaction();
    //    while (cursor->more())
    //    {
    //        mongo::BSONObj itv = cursor->next();
    //        bsonobjects.push_back(itv.copy());
    //    }
    //    x = bsonobjects.begin();
    //    while(x != bsonobjects.end()) {
    //        long long guid_int = (*x).getField("guid_int").numberLong();
    //        long long adv_int = (*x).getField("adv_int").numberLong();
    //        bzero(buf,sizeof(buf));
    //        sqlite3_snprintf(sizeof(buf),buf, "INSERT INTO Informer2OfferRating(id_inf,id_ofr,rating) VALUES('%lld','%lld','%f');", adv_int, guid_int, (*x).getField("full_rating").numberDouble());
    //        try
    //        {
    //            pStmt->SqlStatement(buf);
    //            ++c;
    //        }
    //        catch(Kompex::SQLiteException &ex)
    //        {
    //            logDb(ex);
    //        }
    //        x++;
    //        transCount++;
    //        if (transCount % 1000 == 0)
    //        {
    //            pStmt->CommitTransaction();
    //            pStmt->FreeQuery();
    //            pStmt->BeginTransaction();
    //        }
    //    }
    //    bsonobjects.clear();
    //}
    //catch(std::exception const &ex)
    //{
    //    std::clog<<"["<<pthread_self()<<"]"<<__func__<<" error: "
    //             <<ex.what()
    //             <<" \n"
    //             <<std::endl;
    //}
    //pStmt->CommitTransaction();
    //pStmt->FreeQuery();
    //Log::info("Load inf-rating for %d offers %s",c, query.toString().c_str());
    //delete pStmt;
}

/** Загружает все товарные предложения из MongoDb */
void ParentDB::OfferRatingLoad(const document &query)
{

    Kompex::SQLiteStatement *pStmt;
    //std::vector<mongo::BSONObj> bsonobjects;
    //std::vector<mongo::BSONObj>::const_iterator x;
    //mongo::BSONObj f = BSON("guid"<<1<<"guid_int"<<1<<"full_rating"<<1);
    //auto cursor = monga_main->query(cfg->mongo_main_db_ + ".offer", q_correct, 0, 0, &f);

    //pStmt = new Kompex::SQLiteStatement(pdb);

    //try
    //{
    //    unsigned int transCount = 0;
    //    pStmt->BeginTransaction();
    //    while (cursor->more())
    //    {
    //        mongo::BSONObj itv = cursor->next();
    //        bsonobjects.push_back(itv.copy());
    //    }
    //x = bsonobjects.begin();
    //while(x != bsonobjects.end()) {
    //        std::string id = (*x).getStringField("guid");
    //        if (id.empty())
    //        {
    //            continue;
    //        }

    //        bzero(buf,sizeof(buf));
    //        sqlite3_snprintf(sizeof(buf),buf,
    //"INSERT OR REPLACE INTO Offer2Rating (id, rating) VALUES(%llu,%f);",
    //                         (*x).getField("guid_int").numberLong(),
    //                         (*x).getField("full_rating").numberDouble()
    //                        );

    //        try
    //        {
    //            pStmt->SqlStatement(buf);
    //        }
    //        catch(Kompex::SQLiteException &ex)
    //        {
    //            logDb(ex);
    //        }
    //        x++;
    //        transCount++;
    //        if (transCount % 1000 == 0)
    //        {
    //            pStmt->CommitTransaction();
    //            pStmt->FreeQuery();
    //            pStmt->BeginTransaction();
    //        }

    //    }
    //    bsonobjects.clear();
    //}
    //catch(std::exception const &ex)
    //{
    //    std::clog<<"["<<pthread_self()<<"]"<<__func__<<" error: "
    //             <<ex.what()
    //             <<" \n"
    //             <<std::endl;
    //}


    //pStmt->CommitTransaction();
    //pStmt->FreeQuery();
    delete pStmt;
}

void ParentDB::OfferLoad(const document &query, const nlohmann::json &camp)
{
    Kompex::SQLiteStatement *pStmt;
    unsigned int transCount = 0;
    unsigned int skipped = 0;
    
    pStmt = new Kompex::SQLiteStatement(pdb);

    nlohmann::json o = camp["showConditions"];
    std::string campaignId = camp["guid"].get<std::string>();
    mongocxx::client conn{mongocxx::uri{cfg->mongo_main_url_}};
    conn.read_preference(read_preference(read_preference::read_mode::k_secondary_preferred));
    auto coll = conn[cfg->mongo_main_db_]["offer"];
    auto cursor = coll.find(query.view());

    std::vector<std::string> items;
    try{
        pStmt->BeginTransaction();
        for (auto &&doc : cursor)
        {
               items.push_back(bsoncxx::to_json(doc));
        }
        for(auto i : items) {
            nlohmann::json x = nlohmann::json::parse(i);
            std::string id = x["guid"].get<std::string>();
            if (id.empty())
            {
                skipped++;
                continue;
            }

            std::string image = x["image"].get<std::string>();
            if (image.empty())
            {
                skipped++;
                continue;
            }
    //        bzero(buf,sizeof(buf));
    //        sqlite3_snprintf(sizeof(buf),buf,
    //            "INSERT OR REPLACE INTO Offer (\
    //            id,\
    //            guid,\
    //            retid,\
    //            campaignId,\
    //            image,\
    //            uniqueHits,\
    //            brending,\
    //            description,\
    //            url,\
    //            Recommended,\
    //            recomendet_type,\
    //            recomendet_count,\
    //            title,\
    //            campaign_guid,\
    //            social,\
    //            offer_by_campaign_unique,\
    //            account,\
    //            target,\
    //            UnicImpressionLot,\
    //            html_notification\
    //            )\
    //            VALUES(\
    //                    %llu,\
    //                    '%q',\
    //                    '%q',\
    //                    %llu,\
    //                    '%q',\
    //                    %d,\
    //                    %d,\
    //                    '%q',\
    //                    '%q',\
    //                    '%q',\
    //                    '%q',\
    //                    %d,\
    //                    '%q',\
    //                    '%q',\
    //                    %d,\
    //                    %d,\
    //                    '%q',\
    //                    '%q',\
    //                    %d,\
    //                    %d);",
    //            (*x).getField("guid_int").numberLong(),
    //            id.c_str(),
    //            (*x).getStringField("RetargetingID"),
    //            (*x).getField("campaignId_int").numberLong(),
    //            (*x).getStringField("image"),
    //            (*x).getIntField("uniqueHits"),
    //            o.getBoolField("brending") ? 1 : 0,
    //            (*x).getStringField("description"),
    //            (*x).getStringField("url"),
    //            (*x).getStringField("Recommended"),
    //             o.hasField("recomendet_type") ? o.getStringField("recomendet_type") : "all",
    //             o.hasField("recomendet_count") ? o.getIntField("recomendet_count") : 10,
    //            (*x).getStringField("title"),
    //            campaignId.c_str(),
    //            camp.getBoolField("social") ? 1 : 0,
    //            o.hasField("offer_by_campaign_unique") ? o.getIntField("offer_by_campaign_unique") : 1,
    //            camp.getStringField("account"),
    //            o.getStringField("target"),
    //            o.hasField("UnicImpressionLot") ? o.getIntField("UnicImpressionLot") : 1,
    //            o.getBoolField("html_notification") ? 1 : 0);
    //
            try
            {
                pStmt->SqlStatement(buf);
            }
            catch(Kompex::SQLiteException &ex)
            {
                logDb(ex);
                skipped++;
            }
            transCount++;
            if (transCount % 1000 == 0)
            {
                pStmt->CommitTransaction();
                pStmt->FreeQuery();
                pStmt->BeginTransaction();
            }

        }
        pStmt->CommitTransaction();
        pStmt->FreeQuery();
    }
    catch(std::exception const &ex)
    {
        std::clog<<"["<<pthread_self()<<"]"<<__func__<<" error: "
                 <<ex.what()
                 <<" \n"
                 <<std::endl;
    }

    pStmt->FreeQuery();
    delete pStmt;

    auto filter = document{};
    filter.append(kvp("campaignId", campaignId ));
    OfferRatingLoad(filter);

    Log::info("Loaded %d offers", transCount);
    if (skipped)
        Log::warn("Offers with empty id or image skipped: %d", skipped);
}
void ParentDB::OfferRemove(const std::string &id)
{
    Kompex::SQLiteStatement *pStmt;

    if(id.empty())
    {
        return;
    }

    pStmt = new Kompex::SQLiteStatement(pdb);
    sqlite3_snprintf(sizeof(buf),buf,"DELETE FROM Offer WHERE campaign_guid='%q';",id.c_str());
    try
    {
        pStmt->SqlStatement(buf);
    }
    catch(Kompex::SQLiteException &ex)
    {
        logDb(ex);
    }

    pStmt->FreeQuery();

    delete pStmt;

    Log::info("offer %s removed",id.c_str());
}
//-------------------------------------------------------------------------------------------------------


void ParentDB::logDb(const Kompex::SQLiteException &ex) const
{
    std::clog<<"ParentDB::logDb error: "<<ex.GetString()<<std::endl;
    std::clog<<"ParentDB::logDb request: "<<buf<<std::endl;
    #ifdef DEBUG
    printf("%s\n",ex.GetString().c_str());
    printf("%s\n",buf);
    #endif // DEBUG
}
/** \brief  Закгрузка всех рекламных кампаний из базы данных  Mongo

 */
//==================================================================================
void ParentDB::CampaignLoad(const std::string &aCampaignId)
{
    auto filter = document{};

    if(!aCampaignId.empty())
    {
        filter.append(
                     kvp("showConditions.retargeting", bsoncxx::types::b_bool{true}),
                      kvp("showConditions.retargeting_type", "account"),
                      kvp("status", "working"),
                      kvp("guid", aCampaignId));
    }
    else
    {
        filter.append(
                     kvp("showConditions.retargeting", bsoncxx::types::b_bool{true}),
                      kvp("showConditions.retargeting_type", "account"),
                      kvp("status", "working")
                      );
    }
    CampaignLoad(filter);
}
/** \brief  Закгрузка всех рекламных кампаний из базы данных  Mongo

 */
//==================================================================================
void ParentDB::CampaignLoad(const document &query)
{
    int count = 0;
    mongocxx::client conn{mongocxx::uri{cfg->mongo_main_url_}};
    conn.read_preference(read_preference(read_preference::read_mode::k_secondary_preferred));
    auto coll = conn[cfg->mongo_main_db_]["campaign"];
    auto cursor = coll.find(query.view());

    std::vector<std::string> items;
    try{
        for (auto &&doc : cursor)
        {
               items.push_back(bsoncxx::to_json(doc));
        }
        for(auto i : items) {
            nlohmann::json x = nlohmann::json::parse(i);
            std::string id = x["guid"].get<std::string>();
            if (id.empty())
            {
                Log::warn("Campaign with empty id skipped");
                continue;
            }

            std::string status = x["status"].get<std::string>();
            
            CampaignRemove(id);

            if (status != "working")
            {
                Log::info("Campaign is hold: %s", id.c_str());
                continue;
            }

            //------------------------Create CAMP-----------------------
            //Загрузили все предложения
            auto filter = document{};
            count++;
            filter.append(kvp("campaignId", id ));
            OfferLoad(filter, x);
            Log::info("Loaded campaign: %s", id.c_str());
        }//endfor
    }
    catch(std::exception const &ex)
    {
        std::clog<<"["<<pthread_self()<<"]"<<__func__<<" error: "
                 <<ex.what()
                 <<" \n"
                 <<std::endl;
    }

    Log::info("Loaded %d campaigns",count); 
}


void ParentDB::CampaignRemove(const std::string &CampaignId)
{
    if(CampaignId.empty())
    {
        return;
    }

    Kompex::SQLiteStatement *pStmt;
    pStmt = new Kompex::SQLiteStatement(pdb);
    bzero(buf,sizeof(buf));
    sqlite3_snprintf(sizeof(buf),buf,"DELETE FROM Offer WHERE campaign_guid='%s';",CampaignId.c_str());
    try
    {
        pStmt->SqlStatement(buf);
    }
    catch(Kompex::SQLiteException &ex)
    {
        logDb(ex);
    }

    pStmt->FreeQuery();

    delete pStmt;
}
