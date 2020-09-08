package source.dataip;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import source.helper.ConnectDb;
import source.report.Menu;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

public class ScanHtv extends Thread {

    private static final String MONGO_DATABASE = "htv_new";

    @Override
    public void run() {
        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("HTV1", null,  null);
                process("HTV150", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("HTV1", fromDate,  toDate);
        process("HTV150", fromDate,  toDate);
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
            toDate = (toDate == null) ? new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay() : toDate;
            fromDate = (fromDate == null) ? toDate.minusDays(2) : fromDate;

            for(DateTime currentdate = toDate;
                currentdate.isAfter(fromDate) || currentdate.isEqual(fromDate);
                currentdate = currentdate.minusDays(1)) {
                System.out.println("HTV_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage;
                // Tổng lượt đăng ký tất cả các gói
                listPackage = new String[]{
                        "DKLAI HTV1", "DKLAI HTV150",
                        "DK HTV1", "DK HTV150",
                        "DK HTV1 NOT EM", "DK HTV150 NOT EM"
                };
                int numberNewAll = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói HTV1
                listPackage = new String[]{
                        "DK HTV1"
                };
                int numberNewHTV1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói HTV150
                listPackage = new String[]{
                        "DK HTV150"
                };
                int numberNewHTV150 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói HTV1
                listPackage = new String[]{
                        "DKLAI HTV1"
                };
                int numberAgainHTV1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói HTV150
                listPackage = new String[]{
                        "DKLAI HTV150"
                };
                int numberAgainHTV150 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký thất bại
                listPackage = new String[]{
                        "DK HTV1 NOT EM", "DK HTV150 NOT EM"
                };
                int numberNewFail = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH HTV1", "GH HTV150", "GHMK HTV1", "GHMK HTV150"
                };
                int numberMore = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY HTV1", "HTHUY HTV150",
                        "HUY HTV1", "HUY HTV150"
                };
                int numberCancel = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY HTV1", "HUY HTV150"
                };
                int numberCancelUser = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY HTV1", "HTHUY HTV150",
                };
                int numberCancelSystem = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI HTV1", "DKLAI HTV150",
                        "DK HTV1", "DK HTV150",
                        "GH HTV1", "GH HTV150",
                        "GHMK HTV1", "GHMK HTV150"
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
                insertData.put("registerNewSuccessHTV1", numberNewHTV1);
                insertData.put("registerNewSuccessHTV150", numberNewHTV150);
                insertData.put("registerNewAgainHTV1", numberAgainHTV1);
                insertData.put("registerNewAgainHTV150", numberAgainHTV150);
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
            MongoCollection<Document> collection = database.getCollection("register");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQuery.put("regDatetimeD",
                    new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                            .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
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
            MongoCollection<Document> collection = database.getCollection("register");
            String[] listPackage = new String[]{
                    "DKLAI HTV1", "DKLAI HTV150",
                    "DK HTV1", "DK HTV150",
                    "GH HTV1", "GH HTV150",
                    "GHMK HTV1", "GHMK HTV150"
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("commandCode",
                                new BasicDBObject("$in", listPackage)
                        ).append("regDatetimeD",
                                new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                                        .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
                        )
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("commandCode",
                                new BasicDBObject("$in", listPackage)
                        ).append("regDatetimeD",
                                new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                                        .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
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
            MongoCollection<Document> collection = database.getCollection("register");
            String[] listPackage = new String[]{
                    "HTHUY HTV1", "HTHUY HTV150",
                    "HUY HTV1", "HUY HTV150",
            };
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQuery.put("regDatetimeD",
                    new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                            .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
            );
            searchQuery.put("endDatetimeD",
                    new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                            .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
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
        try {
            // Lấy danh sách số đã đăng ký
            MongoCollection<Document> collectionCancel = database.getCollection("register");
            String[] listPackage = new String[]{
                    "DK HTV1", "DK HTV150",
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "HTV-DATA"
                        ).append("regDatetimeT",
                                new BasicDBObject("$lte", (datetime.plusDays(1).getMillis() / 1000))
                                        .append("$gte", (datetime.minusDays(30).getMillis() / 1000))
                        ).append("status", "3")
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "HTV-DATA"
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
            MongoCollection<Document> collection = database.getCollection("register");
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

}
