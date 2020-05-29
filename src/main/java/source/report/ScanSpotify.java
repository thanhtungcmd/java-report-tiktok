package source.report;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import source.helper.ConnectDb;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

public class ScanSpotify extends Thread {

    @Override
    public void run() {
        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("SF1", null,  null);
                process("SF7", null,  null);
                process("SF30", null,  null);
                process("SF80", null,  null);
                System.out.println("Spotify-Thread run:" + new Date());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("SF1", fromDate,  toDate);
        process("SF7", fromDate,  toDate);
        process("SF30", fromDate,  toDate);
        process("SF80", fromDate,  toDate);
    }

    private void sleepTime() {
        try {
            for(int i = 0; i < 360; i++) {
                if(Menu.isRunning) {
                    Thread.sleep(10000);
                } else {
                    break;
                }
            }
        } catch(Exception ignore) {}
    }

    public static void process(String packageFilter, DateTime fromDate, DateTime toDate) {
        MongoClient conn = null;
        try {
            conn = ConnectDb.createConnection();
            MongoDatabase database = conn.getDatabase(ConnectDb.MONGO_DATABASE);
            toDate = (toDate == null) ? new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay() : toDate;
            fromDate = (fromDate == null) ? toDate.minusDays(2) : fromDate;

            DateTime dateApply = new DateTime(2020, 5, 27, 0, 0);
            if (fromDate.getMillis() < dateApply.getMillis()) {
                return;
            }

            for(DateTime currentdate = toDate;
                currentdate.isAfter(fromDate) || currentdate.isEqual(fromDate);
                currentdate = currentdate.minusDays(1)) {
                System.out.println("SPOTIFY_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage;
                // Tổng lượt đăng ký tất cả các gói
                listPackage = new String[]{
                        "DKLAI SF1", "DKLAI SF7", "DKLAI SF30", "DKLAI SF80",
                        "DK SF1", "DK SF7", "DK SF30", "DK SF80",
                        "DKFREE SF1", "DKFREE SF7", "DKFREE SF30", "DKFREE SF80",
                        "DK SF1 NOT EM", "DK SF7 NOT EM", "DK SF30 NOT EM", "DK SF80 NOT EM"
                };
                int numberNewAll = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF1
                listPackage = new String[]{
                        "DK SF1"
                };
                int numberNewSF1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF7
                listPackage = new String[]{
                        "DK SF7"
                };
                int numberNewSF7 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF30
                listPackage = new String[]{
                        "DK SF30"
                };
                int numberNewSF30 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF80
                listPackage = new String[]{
                        "DK SF80"
                };
                int numberNewSF80 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF1
                listPackage = new String[]{
                        "DKLAI SF1"
                };
                int numberAgainSF1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF7
                listPackage = new String[]{
                        "DKLAI SF7"
                };
                int numberAgainSF7 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF30
                listPackage = new String[]{
                        "DKLAI SF30"
                };
                int numberAgainSF30 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF80
                listPackage = new String[]{
                        "DKLAI SF80"
                };
                int numberAgainSF80 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DKFREE SF1", "DKFREE SF7", "DKFREE SF30", "DKFREE SF80",
                };
                int numberNewFree = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DK SF1 NOT EM", "DK SF7 NOT EM", "DK SF30 NOT EM", "DK SF80 NOT EM"
                };
                int numberNewFail = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH SF1", "GH SF7", "GH SF30", "GH SF80"
                };
                int numberMore = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY SF1", "HTHUY SF7", "HTHUY SF30", "HTHUY SF80",
                        "HUY SF1", "HUY SF7", "HUY SF30", "HUY SF80"
                };
                int numberCancel = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY SF1", "HUY SF7", "HUY SF30", "HUY SF80"
                };
                int numberCancelUser = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY SF1", "HTHUY SF7", "HTHUY SF30", "HTHUY SF80",
                };
                int numberCancelSystem = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI SF1", "DKLAI SF7", "DKLAI SF30", "DKLAI SF80",
                        "DK SF1", "DK SF7", "DK SF30", "DK SF80",
                        "GH SF1", "GH SF7", "GH SF30", "GH SF80"
                };
                int numberPSC = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng doanh thu
                int numberRevenue = sumRevenue(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký hủy trong ngày
                int numberInday = countInday(database, currentdate, packageFilter);

                // Insert Db
                MongoCollection<Document> collection = database.getCollection("report_spotify");
                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("datetime", new Timestamp(currentdate.getMillis()));
                searchQuery.put("package", packageFilter);
                Document data = collection.find(searchQuery).first();
                Document insertData = new Document();
                insertData.put("datetime", new Timestamp(currentdate.getMillis()));
                insertData.put("registerNewSuccessTT1", numberNewSF1);
                insertData.put("registerNewSuccessTT7", numberNewSF7);
                insertData.put("registerNewSuccessTT30", numberNewSF30);
                insertData.put("registerNewSuccessTT80", numberNewSF80);
                insertData.put("registerNewAgainTT1", numberAgainSF1);
                insertData.put("registerNewAgainTT7", numberAgainSF7);
                insertData.put("registerNewAgainTT30", numberAgainSF30);
                insertData.put("registerNewAgainTT80", numberAgainSF80);
                insertData.put("registerNewFree", numberNewFree);
                insertData.put("registerNewFalse", numberNewFail);
                insertData.put("registerMore", numberMore);
                insertData.put("registerCancle", numberCancel);
                insertData.put("registerCancleUser", numberCancelUser);
                insertData.put("registerCancleSystem", numberCancelSystem);
                insertData.put("registerPSC", numberPSC);
                insertData.put("registerRevenue", numberRevenue);
                insertData.put("registerInDay", numberInday);
                insertData.put("registerLogAll", numberNewAll);
                if (packageFilter != null) {
                    insertData.put("package", packageFilter);
                }

                if ( data == null ) {
                    collection.insertOne(insertData);
                } else {
                    collection.updateOne(searchQuery, new Document("$set", insertData));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ConnectDb.closeConnection(conn);
        }
    }

    private static int countRegister(MongoDatabase database, DateTime datetime, String[] listPackage, String packageFilter) {
        int output = 0;
        try {
            MongoCollection<Document> collection = collectionDbDate(database, datetime);
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQuery.put("regDatetimeT",
                    new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                            .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
            );
            if (packageFilter != null) {
                searchQuery.put("packageCode", packageFilter);
            }
            output = (int) collection.count(searchQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static int sumRevenue(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        try {
            MongoCollection<Document> collection = collectionDbDate(database, datetime);
            String[] listPackage = new String[]{
                    "DKLAI SF1", "DKLAI SF7", "DKLAI SF30", "DKLAI SF80",
                    "DK SF1", "DK SF7", "DK SF30", "DK SF80",
                    "GH SF1", "GH SF7", "GH SF30", "GH SF80"
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("commandCode",
                                new BasicDBObject("$in", listPackage)
                        ).append("regDatetimeT",
                                new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                                        .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
                        )
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("commandCode",
                                new BasicDBObject("$in", listPackage)
                        ).append("regDatetimeT",
                                new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                                        .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
                        ).append("packageCode", packageFilter)
                );
            }
            AggregateIterable<Document> searchQuery = collection.aggregate(Arrays.asList(
                    match,
                    new BasicDBObject("$group",
                            new BasicDBObject("_id",
                                    new BasicDBObject("msisdn", "")
                            )
                                    .append("sum",
                                            new BasicDBObject("$sum", "$charge_priceInt")
                                    )
                    )
            ));

            for (Document dbObject : searchQuery)
            {
                output = (int) dbObject.get("sum");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static int countInday(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        try {
            MongoCollection<Document> collection = collectionDbDate(database, datetime);
            String[] listPackage = new String[]{
                    "HTHUY SF1", "HTHUY SF7", "HTHUY SF30", "HTHUY SF80",
                    "HUY SF1", "HUY SF7", "HUY SF30", "HUY SF80"
            };
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQuery.put("regDatetimeT",
                    new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                            .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
            );
            searchQuery.put("endDatetimeT",
                    new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                            .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
            );
            if (packageFilter != null) {
                searchQuery.put("packageCode", packageFilter);
            }
            output = (int) collection.count(searchQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static MongoCollection<Document> collectionDbDate(MongoDatabase database, DateTime dateTime) {
        MongoCollection<Document> collection;

        DateTime dateApply = new DateTime(2020, 5, 27, 0, 0);
        if (dateTime.getMillis() < dateApply.getMillis()) {
            collection = null;
        } else {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMM");
            String strTime = formatter.print(dateTime);
            collection = database.getCollection("register_"+ strTime);
        }

        return collection;
    }

}
