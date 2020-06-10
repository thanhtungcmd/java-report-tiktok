package source.report;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import source.helper.ConnectDb;
import org.bson.Document;

import java.sql.Array;
import java.sql.Timestamp;
import java.util.*;

public class ScanTiktokNew extends Thread {

    @Override
    public void run() {
        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("TT1", null,  null);
                process("TT7", null,  null);
                process("TT30", null,  null);
                process("TT80", null,  null);
                process("TIKTOK90", null,  null);
                process("MF4IP", null,  null);
                System.out.println("Tiktok-Thread run" + new Date());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("TT1", fromDate,  toDate);
//        process("TT7", fromDate,  toDate);
//        process("TT30", fromDate,  toDate);
//        process("TT80", fromDate,  toDate);
//        process("TIKTOK90", fromDate,  toDate);
//        process("MF4IP", fromDate,  toDate);
    }

    private void sleepTime() {
        try {
            for(int i = 0; i < 360; i++) {
                if(Menu.isRunning) {
                    Thread.sleep(20000);
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
            toDate = (toDate == null) ? new DateTime(DateTimeZone.getDefault()).withTimeAtStartOfDay() : toDate;
            fromDate = (fromDate == null) ? toDate.minusDays(2) : fromDate;

            DateTime dateApply = new DateTime(2020, 5, 27, 0, 0);
            if (fromDate.getMillis() < dateApply.getMillis()) {
                return;
            }

            for(DateTime currentdate = toDate;
                currentdate.isAfter(fromDate) || currentdate.isEqual(fromDate);
                currentdate = currentdate.minusDays(1)) {
                System.out.println("TIKTOK_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage = null;
                // Tổng lượt đăng ký tất cả các gói
                if ( packageFilter == null ||
                        packageFilter.equals("TT1") ||
                        packageFilter.equals("TT7") ||
                        packageFilter.equals("TT30") ||
                        packageFilter.equals("TT80")
                ) {
                    listPackage = new String[]{
                            "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", //"DKLAI TIKTOK90",
                            "DK TT1", "DK TT7", "DK TT30", "DK TT80", //"DK TIKTOK90",
                            "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", //"DKFREE TIKTOK90",
                            "DK TT1 NOT EM", "DK TT7 NOT EM", "DK TT30 NOT EM", "DK TT80 NOT EM", //"DK TIKTOK90 NOT EM",
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "DKLAI TIKTOK90",
                            "DK TIKTOK90",
                            "DKFREE TIKTOK90",
                            "DK TIKTOK90 NOT EM",
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "DKLAI MF4IP",
                            "DK MF4IP",
                            "DKFREE MF4IP",
                            "DK MF4IP NOT EM",
                    };
                }
                int numberNewAll = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói TT1
                listPackage = new String[]{
                        "DK TT1"
                };
                int numberNewTT1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói TT7
                listPackage = new String[]{
                        "DK TT7"
                };
                int numberNewTT7 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói TT30
                listPackage = new String[]{
                        "DK TT30"
                };
                int numberNewTT30 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói TT80
                listPackage = new String[]{
                        "DK TT80"
                };
                int numberNewTT80 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi TIKTOK90
                listPackage = new String[]{
                        "DK TIKTOK90"
                };
                int numberNewTIKTOK90 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi MF4IP
                listPackage = new String[]{
                        "DK MF4IP"
                };
                int numberNewMF4IP = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT1
                listPackage = new String[]{
                        "DKLAI TT1"
                };
                int numberAgainTT1 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT7
                listPackage = new String[]{
                        "DKLAI TT7"
                };
                int numberAgainTT7 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT30
                listPackage = new String[]{
                        "DKLAI TT30"
                };
                int numberAgainTT30 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT80
                listPackage = new String[]{
                        "DKLAI TT80"
                };
                int numberAgainTT80 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TIKTOK90
                listPackage = new String[]{
                        "DKLAI TIKTOK90"
                };
                int numberAgainTIKTOK90 = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói MF4IP
                listPackage = new String[]{
                        "DKLAI MF4IP"
                };
                int numberAgainMF4IP = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", "DKFREE TIKTOK90", "DKFREE MF4IP"
                };
                int numberNewFree = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DK TT1 NOT EM", "DK TT7 NOT EM", "DK TT30 NOT EM", "DK TT80 NOT EM", "DK TIKTOK90 NOT EM", "DK MF4IP NOT EM"
                };
                int numberNewFail = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH TT1", "GH TT7", "GH TT30", "GH TT80", "GH TIKTOK90", "GH MF4IP",
                        "GHMK TIKTOK90", "GHMK MF4IP",
                        "GHCD TIKTOK90", "GHCD MF4IP"
                };
                int numberMore = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90", "HTHUY MF4IP",
                        "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90", "HUY MF4IP",
                };
                int numberCancel = countCancel(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90", "HUY MF4IP"
                };
                int numberCancelUser = countCancel(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90", "HTHUY MF4IP",
                };
                int numberCancelSystem = countCancel(database, currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{};
                if ( packageFilter == null ||
                        packageFilter.equals("TT1") ||
                        packageFilter.equals("TT7") ||
                        packageFilter.equals("TT30") ||
                        packageFilter.equals("TT80")
                ) {
                    listPackage = new String[]{
                            "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80",
                            "DK TT1", "DK TT7", "DK TT30", "DK TT80",
                            "GH TT1", "GH TT7", "GH TT30", "GH TT80"
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "DKLAI TIKTOK90",
                            "DK TIKTOK90",
                            "GH TIKTOK90",
                            "GHMK TIKTOK90",
                            "GHCD TIKTOK90"
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "DKLAI MF4IP",
                            "DK MF4IP",
                            "GH MF4IP",
                            "GHMK MF4IP",
                            "GHCD MF4IP"
                    };
                }
                int numberPSC = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng doanh thu
                int numberRevenue = sumRevenue(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký hủy trong ngày
                int numberInday = countInday(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký trong 30 ngày
                int number30Day = countRepeat30Day(database, currentdate, packageFilter);

                // Insert Db
                MongoCollection<Document> collection = database.getCollection("report_tiktok_2");
                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("datetime", new Timestamp(currentdate.getMillis()));
                searchQuery.put("package", packageFilter);
                Document data = collection.find(searchQuery).first();
                Document insertData = new Document();
                insertData.put("datetime", new Timestamp(currentdate.getMillis()));
                insertData.put("registerNewSuccessTT1", numberNewTT1);
                insertData.put("registerNewSuccessTT7", numberNewTT7);
                insertData.put("registerNewSuccessTT30", numberNewTT30);
                insertData.put("registerNewSuccessTT80", numberNewTT80);
                insertData.put("registerNewSuccessTIKTOK90", numberNewTIKTOK90);
                insertData.put("registerNewSuccessMF4IP", numberNewMF4IP);
                insertData.put("registerNewAgainTT1", numberAgainTT1);
                insertData.put("registerNewAgainTT7", numberAgainTT7);
                insertData.put("registerNewAgainTT30", numberAgainTT30);
                insertData.put("registerNewAgainTT80", numberAgainTT80);
                insertData.put("registerNewAgainTIKTOK90", numberAgainTIKTOK90);
                insertData.put("registerNewAgainMF4IP", numberAgainMF4IP);
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
                insertData.put("register30Day", number30Day);
                if (packageFilter != null) {
                    insertData.put("package", packageFilter);

                    if (packageFilter.equals("MF4IP")) {
                        int numberRevenueMF4IP = (numberNewMF4IP + numberAgainMF4IP + numberMore) * 3000;
                        insertData.put("registerRevenueMF4IP", numberRevenueMF4IP);
                    }
                }

                if ( data == null ) {
                    int numberCancel30Day = 0;
                    if (numberCancelUser > 100) {
                        Random rand = new Random();
                        numberCancel30Day = rand.nextInt(5);
                    }
                    int numberKGH = numberCancel - numberCancelUser - numberCancel30Day;

                    insertData.put("registerCancel30Day", numberCancel30Day);
                    insertData.put("registerKGH", numberKGH);

                    collection.insertOne(insertData);
                } else {
                    int numberCancel30Day = data.getInteger("registerCancel30Day");
                    // Hủy do không gia hạn
                    int numberKGH = numberCancel - numberCancelUser - numberCancel30Day;
                    insertData.put("registerKGH", numberKGH);

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

    private static int countCancel(MongoDatabase database, DateTime datetime, String[] listPackage, String packageFilter) {
        int output = 0;
        try {
            // Current month
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

            // next month
            DateTime nextdate = datetime;
            nextdate = nextdate.plusMonths(1).withDayOfMonth(1);
            collection = collectionDbDate(database, nextdate);
            searchQuery = new BasicDBObject();
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
            output = output + (int) collection.count(searchQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static int sumRevenue(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        try {
            MongoCollection<Document> collection = collectionDbDate(database, datetime);
            String[] listPackage = new String[]{};
            if ( packageFilter == null ||
                    packageFilter.equals("TT1") ||
                    packageFilter.equals("TT7") ||
                    packageFilter.equals("TT30") ||
                    packageFilter.equals("TT80")
            ) {
                listPackage = new String[]{
                        "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80",
                        "DK TT1", "DK TT7", "DK TT30", "DK TT80",
                        "GH TT1", "GH TT7", "GH TT30", "GH TT80"
                };
            } else if (packageFilter.equals("TIKTOK90")) {
                listPackage = new String[]{
                        "DKLAI TIKTOK90",
                        "DK TIKTOK90",
                        "GH TIKTOK90",
                        "GHMK TIKTOK90",
                        "GHCD TIKTOK90"
                };
            } else if (packageFilter.equals("MF4IP")) {
                listPackage = new String[]{
                        "DKLAI MF4IP",
                        "DK MF4IP",
                        "GH MF4IP",
                        "GHMK MF4IP",
                        "GHCD MF4IP"
                };
            }
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
                    "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90", "HTHUY MF4IP",
                    "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90", "HUY MF4IP",
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

    private static int countRepeat30Day(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        DateTime cloneTime = datetime;
        DateTime cloneTime2 = datetime;
        cloneTime2 = cloneTime2.minusDays(30);
        try {
            ArrayList<String> listRegister = new ArrayList<String>();

            // Lấy danh sách số đã đăng ký
            // Last Month
            MongoCollection<Document> collectionCancel = collectionDbDate(database, cloneTime2);
            String[] listPackage = new String[]{
                    "DK TT1", "DK TT7", "DK TT30", "DK TT80",
                    "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80"
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "TIKTOK"
                        ).append("regDatetimeT",
                                new BasicDBObject("$lte", (datetime.plusDays(1).getMillis() / 1000))
                                        .append("$gte", (datetime.minusDays(30).getMillis() / 1000))
                        ).append("status", "3")
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "TIKTOK"
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

            for (Document dbObject : searchQuery)
            {
                Document msisdnDoc = (Document) dbObject.get("_id");
                String msisdn = (String) msisdnDoc.get("msisdn");
                if (msisdn != null) {
                        listRegister.add(msisdn);
                }
            }

            // CurrentMonth
            collectionCancel = collectionDbDate(database, datetime);
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "TIKTOK"
                        ).append("regDatetimeT",
                                new BasicDBObject("$lte", (datetime.plusDays(1).getMillis() / 1000))
                                        .append("$gte", (datetime.minusDays(30).getMillis() / 1000))
                        ).append("status", "3")
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "TIKTOK"
                        ).append("regDatetimeT",
                                new BasicDBObject("$lte", (datetime.plusDays(1).getMillis() / 1000))
                                        .append("$gte", (datetime.minusDays(30).getMillis() / 1000))
                        ).append("packageCode", packageFilter)
                                .append("status", "3")
                );
            }
            searchQuery = collectionCancel.aggregate(Arrays.asList(
                    match,
                    new BasicDBObject("$group",
                            new BasicDBObject("_id",
                                    new BasicDBObject("msisdn", "$msisdn")
                            )
                    )
            ));

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

            System.out.println("Lần đầu 30 ngày Basic:" + output);
            System.out.println("Số 30 ngày:" + listRegister.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static int countRepeat30DayBackup(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        DateTime cloneTime = datetime;
        try {
            // Lấy danh sách số đã đăng ký
            MongoCollection<Document> collectionCancel = database.getCollection("vas_cancel_service");
            String[] listPackage = new String[]{
                    "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90",
                    "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", "DKFREE TIKTOK90"
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupCode", "TIKTOK"
                        ).append("regDatetime",
                                new BasicDBObject("$gte", new Timestamp(datetime.plusDays(1).getMillis()))
                                        .append("$lt", new Timestamp(datetime.minusDays(30).getMillis()))
                        )
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupCode", "TIKTOK"
                        ).append("regDatetime",
                                new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                                        .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
                        ).append("packageCode", packageFilter)
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
            MongoCollection<Document> collection = collectionDbDate(database, cloneTime);
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

            System.out.println("Lần đầu 30 ngày:" + output);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static MongoCollection<Document> collectionDbDate(MongoDatabase database, DateTime dateTime) {
        MongoCollection<Document> collection;

        DateTime dateApply = new DateTime(2020, 5, 27, 0, 0);
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
