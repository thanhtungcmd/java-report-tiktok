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
import java.util.ArrayList;
import java.util.Arrays;

public class ScanThvlNew extends Thread {

    private static final String MONGO_DATABASE = "thvl_new";

    @Override
    public void run() {
        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("VL1", null,  null);
                process("VL30", null,  null);
                process("VL80", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("VL1", fromDate,  toDate);
        process("VL30", fromDate,  toDate);
        process("VL80", fromDate,  toDate);
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
            MongoDatabase database = conn.getDatabase(MONGO_DATABASE);
            toDate = (toDate == null) ? new DateTime(DateTimeZone.getDefault()).withTimeAtStartOfDay() : toDate;
            fromDate = (fromDate == null) ? toDate.minusDays(2) : fromDate;

            DateTime dateApply = new DateTime(2020, 6, 10, 0, 0);
            if (fromDate.getMillis() < dateApply.getMillis()) {
                fromDate = new DateTime(2020, 6, 10, 0, 0);
            }

            for(DateTime currentdate = toDate;
                currentdate.isAfter(fromDate) || currentdate.isEqual(fromDate);
                currentdate = currentdate.minusDays(1)) {
                System.out.println("THVL_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage;
                // Tổng lượt đăng ký tất cả các gói
                listPackage = new String[]{
                        "DKLAI VL1", "DKLAI VL30", "DKLAI VL80",
                        "DK VL1", "DK VL30", "DK VL80",
                        "DK VL1 NOT EM", "DK VL30 NOT EM", "DK VL80 NOT EM"
                };
                int numberNewAll = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói VL1
                listPackage = new String[]{
                        "DK VL1"
                };
                int numberNewVL1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói VL30
                listPackage = new String[]{
                        "DK VL30"
                };
                int numberNewVL30 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói VL80
                listPackage = new String[]{
                        "DK VL80"
                };
                int numberNewVL80 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói VL1
                listPackage = new String[]{
                        "DKLAI VL1"
                };
                int numberAgainVL1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói VL30
                listPackage = new String[]{
                        "DKLAI VL30"
                };
                int numberAgainVL30 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói VL80
                listPackage = new String[]{
                        "DKLAI VL80"
                };
                int numberAgainVL80 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký thất bại
                listPackage = new String[]{
                        "DK VL1 NOT EM", "DK VL30 NOT EM", "DK VL80 NOT EM"
                };
                int numberNewFail = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH VL1", "GH VL30", "GH VL80",
                        "GHMK VL1", "GHMK VL30", "GHMK VL80"
                };
                int numberMore = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY VL1", "HTHUY VL30", "HTHUY VL80",
                        "HUY VL1", "HUY VL30", "HUY VL80"
                };
                int numberCancel = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY VL1", "HUY VL80", "HUY VL80"
                };
                int numberCancelUser = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY VL1", "HTHUY VL30",  "HTHUY VL80"
                };
                int numberCancelSystem = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI VL1", "DKLAI VL30", "DKLAI VL80",
                        "DK VL1", "DK VL30", "DK VL80",
                        "GH VL1", "GH VL30", "GH VL80",
                        "GHMK VL1", "GHMK VL30", "GHMK VL80",
                };
                int numberPSC = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng doanh thu
                int numberRevenue = sumRevenue(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký hủy trong ngày
                int numberInday = countInday(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký trong 30 ngày
                int number30Day = countRepeat30Day(database, currentdate, packageFilter);

                // Insert Db
                MongoCollection<Document> collection = database.getCollection("report");
                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("datetime", new Timestamp(currentdate.getMillis()));
                searchQuery.put("package", packageFilter);
                Document data = collection.find(searchQuery).first();
                Document insertData = new Document();
                insertData.put("datetime", new Timestamp(currentdate.getMillis()));
                insertData.put("registerNewSuccessVL1", numberNewVL1);
                insertData.put("registerNewSuccessVL30", numberNewVL30);
                insertData.put("registerNewSuccessVL80", numberNewVL80);
                insertData.put("registerNewAgainVL1", numberAgainVL1);
                insertData.put("registerNewAgainVL30", numberAgainVL30);
                insertData.put("registerNewAgainVL80", numberAgainVL80);
                insertData.put("registerNewFalse", numberNewFail);
                insertData.put("registerMore", numberMore);
                insertData.put("registerCancle", numberCancel);
                insertData.put("registerCancleUser", numberCancelUser);
                insertData.put("registerCancleSystem", numberCancelSystem);
                insertData.put("registerPSC", numberPSC);
                insertData.put("registerRevenue", numberRevenue);
                insertData.put("registerInDay", numberInday);
                insertData.put("registerLogAll", numberNewAll);
                insertData.put("register30Day", number30Day);
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
                    "DKLAI VL1", "DKLAI VL30", "DKLAI VL80",
                    "DK VL1", "DK VL30", "DK VL80",
                    "GH VL1", "GH VL30", "GH VL80",
                    "GHMK VL1", "GHMK VL30", "GHMK VL80"
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
                    "HTHUY VL1", "HTHUY VL30",
                    "HUY VL1", "HUY VL30",
            };
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQuery.put("regDatetimeT",
                    new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                            .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
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

    private static int countRepeat30Day(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        DateTime cloneTime = datetime;
        DateTime cloneTime2 = datetime;
        try {
            // Lấy danh sách số đã đăng ký
            MongoCollection<Document> collectionCancel = collectionDbDate(database, datetime);
            String[] listPackage = new String[]{
                    "DK VL1", "DK VL30", "DK VL80",
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "THVL-DATA"
                        ).append("regDatetimeT",
                                new BasicDBObject("$lte", (datetime.plusDays(1).getMillis() / 1000))
                                        .append("$gte", (datetime.minusDays(30).getMillis() / 1000))
                        ).append("status", "3")
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "THVL-DATA"
                        ).append("regDatetimeT",
                                new BasicDBObject("$lte", (datetime.plusDays(1).getMillis() / 1000))
                                        .append("$gte", (datetime.minusDays(30).getMillis() / 1000))
                        ).append("packageCode", packageFilter)
                                .append("status", "3")
                );
            }
            AggregateIterable<Document> searchQuery = collectionCancel.aggregate(Arrays.asList(
                    match,
                    new BasicDBObject("$group",
                            new BasicDBObject("_id",
                                    new BasicDBObject("msisdn", "$msisdn")
                            )
                    )
            ));

            ArrayList<String> listRegister = new ArrayList<String>();
            for (Document dbObject : searchQuery)
            {
                Document msisdnDoc = (Document) dbObject.get("_id");
                String msisdn = (String) msisdnDoc.get("msisdn");
                if (msisdn != null) {
                    listRegister.add(msisdn);
                }
            }

            // Lấy danh sách
            MongoCollection<Document> collection = collectionDbDate(database, datetime);
            BasicDBObject searchQueryData = new BasicDBObject();
            searchQueryData.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQueryData.put("regDatetimeT",
                    new BasicDBObject("$gte", (datetime.getMillis() / 1000))
                            .append("$lt", (datetime.plusDays(1).getMillis() / 1000))
            );
            searchQueryData.put("msisdn",
                    new BasicDBObject("$nin", listRegister)
            );
            if (packageFilter != null) {
                searchQueryData.put("packageCode", packageFilter);
            }
            output = (int) collection.count(searchQueryData);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static MongoCollection<Document> collectionDbDate(MongoDatabase database, DateTime dateTime) {
        MongoCollection<Document> collection;

        DateTime dateApply = new DateTime(2020, 6, 10, 0, 0);
        if (dateTime.getMillis() < dateApply.getMillis()) {
            collection = database.getCollection("register");
        } else {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMM");
            String strTime = formatter.print(dateTime);
            collection = database.getCollection("register_"+ strTime);
        }

        return collection;
    }

}
