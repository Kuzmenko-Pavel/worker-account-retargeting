#ifndef PARENTDB_H
#define PARENTDB_H
#include <bsoncxx/builder/basic/document.hpp>
#include <KompexSQLiteDatabase.h>
#include <KompexSQLiteException.h>
#include "json.h"

using bsoncxx::builder::basic::document;

class ParentDB
{
public:
    ParentDB();
    virtual ~ParentDB();

    void loadRating(const std::string &id="");
    void OfferRatingLoad(const document &query);
    void OfferLoad(const document &query, const nlohmann::json &camp);
    void OfferRemove(const std::string &id);
    void CampaignLoad(const std::string &campaignId = std::string());
    void CampaignLoad(const document &query);
    void CampaignRemove(const std::string &aCampaignId);

private:
    Kompex::SQLiteDatabase *pdb;
    char buf[262144];
    void logDb(const Kompex::SQLiteException &ex) const;
};

#endif // PARENTDB_H
