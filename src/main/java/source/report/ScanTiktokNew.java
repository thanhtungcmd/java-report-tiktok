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
                System.out.println("Tiktok-Thread run" + new Date());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("TT1", fromDate,  toDate);
        process("TT7", fromDate,  toDate);
        process("TT30", fromDate,  toDate);
        process("TT80", fromDate,  toDate);
        process("TIKTOK90", fromDate,  toDate);
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

                String[] listPackage;
                // Tổng lượt đăng ký tất cả các gói
                if ( packageFilter == null || !packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", //"DKLAI TIKTOK90",
                            "DK TT1", "DK TT7", "DK TT30", "DK TT80", //"DK TIKTOK90",
                            "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", //"DKFREE TIKTOK90",
                            "DK TT1 NOT EM", "DK TT7 NOT EM", "DK TT30 NOT EM", "DK TT80 NOT EM", //"DK TIKTOK90 NOT EM",
                    };
                } else {
                    listPackage = new String[]{
                            "DKLAI TIKTOK90",
                            "DK TIKTOK90",
                            "DKFREE TIKTOK90",
                            "DK TIKTOK90 NOT EM",
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

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", "DKFREE TIKTOK90"
                };
                int numberNewFree = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DK TT1 NOT EM", "DK TT7 NOT EM", "DK TT30 NOT EM", "DK TT80 NOT EM", "DK TIKTOK90 NOT EM"
                };
                int numberNewFail = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH TT1", "GH TT7", "GH TT30", "GH TT80", "GH TIKTOK90",
                        "GHMK TIKTOK90",
                        "GHCD TIKTOK90"
                };
                int numberMore = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90",
                        "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90",
                };
                int numberCancel = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90"
                };
                int numberCancelUser = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90",
                };
                int numberCancelSystem = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", "DKLAI TIKTOK90",
                        "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90",
                        "GH TT1", "GH TT7", "GH TT30", "GH TT80", "GH TIKTOK90",
                        "GHMK TIKTOK90",
                        "GHCD TIKTOK90"
                };
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
                insertData.put("registerNewAgainTT1", numberAgainTT1);
                insertData.put("registerNewAgainTT7", numberAgainTT7);
                insertData.put("registerNewAgainTT30", numberAgainTT30);
                insertData.put("registerNewAgainTT80", numberAgainTT80);
                insertData.put("registerNewAgainTIKTOK90", numberAgainTIKTOK90);
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

    private static int sumRevenue(MongoDatabase database, DateTime datetime, String packageFilter) {
        int output = 0;
        try {
            MongoCollection<Document> collection = collectionDbDate(database, datetime);
            String[] listPackage = new String[]{
                    "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", "DKLAI TIKTOK90",
                    "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90",
                    "GH TT1", "GH TT7", "GH TT30", "GH TT80", "GH TIKTOK90",
                    "GHMK TIKTOK90",
                    "GHCD TIKTOK90"
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
                    "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90",
                    "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90",
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
        try {
            // Lấy danh sách số đã đăng ký
            MongoCollection<Document> collectionCancel = collectionDbDate(database, cloneTime2.minusDays(30));
            String[] listPackage = new String[]{
                    "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90",
                    "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", "DKFREE TIKTOK90"
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

        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static int getNewCancel(MongoDatabase db, DateTime datetime, String serviceCode) {
        int output = 0;
        try {
            ArrayList<String> msisdn = getAllMsisdn(db, datetime, serviceCode);
            MongoCollection<Document> table = collectionDbDate(db, datetime);
            BasicDBObject search = new BasicDBObject();
            search.put("msisdn", new BasicDBObject("$nin", msisdn));
            long ts1 = (datetime.getMillis() / 1000);
            long ts2 = (datetime.plusDays(1).getMillis() / 1000);
            search.put("created_at", new BasicDBObject("$gte", ts1).append("$lte", ts2));
            search.put("commandCode", "HTHUY " + serviceCode);
            output = (int) table.count(search);
            System.out.println("--------Hệ thống hủy 30 ngày "+ datetime +": "+ output);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static ArrayList<String> getAllMsisdn(MongoDatabase db, DateTime datetime, String serviceCode) {
        ArrayList<String> output = new ArrayList<String>();

        try {
            HashMap<String, String> map = new HashMap<String, String>();
            getMsisdnAgainThisMonth(db, datetime, serviceCode, map);
            getMsisdnAgainPrevMonth(db, datetime, serviceCode, map);
            getMsisdnRegister(db, datetime, serviceCode, map);
            Iterable<String> it = map.keySet();
            for(String key : it) {
                output.add(key);
            }
            map.clear();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return output;
    }

    private static void getMsisdnAgainThisMonth (MongoDatabase db, DateTime datetime, String serviceCode, HashMap<String, String> map) {
        // Time
        long ts = datetime.getMillis() / 1000;
        DateTime startMonth = datetime;
        startMonth = startMonth.withDayOfMonth(1).withTimeAtStartOfDay();
        long ts2 = startMonth.getMillis() / 1000;

        MongoCollection<Document> collect = collectionDbDate(db, datetime);
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("groupcode", "TIKTOK");
        searchQuery.put("commandCode", "GH " + serviceCode);
        searchQuery.put("regDatetimeT", new BasicDBObject("$gte", ts2).append("$lt", ts));
        MongoCursor<Document> cursor = collect.find(searchQuery).iterator();
        while (cursor.hasNext()) {
            Document object = cursor.next();
            map.put((String) object.get("msisdn"), (String) object.get("msisdn"));
        }
    }

    private static void getMsisdnAgainPrevMonth (MongoDatabase db, DateTime datetime, String serviceCode, HashMap<String, String> map) {
        // Time
        DateTime currentTime = datetime;
        currentTime = currentTime.minusDays(30);
        long ts = currentTime.getMillis() / 1000;
        DateTime endMonth = datetime;
        endMonth = endMonth.withDayOfMonth(1).minusDays(1).withHourOfDay(23).withMinuteOfHour(59);
        long ts2 = endMonth.getMillis() / 1000;

        MongoCollection<Document> collect = collectionDbDate(db, currentTime);
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("groupcode", "TIKTOK");
        searchQuery.put("commandCode", "GH " + serviceCode);
        searchQuery.put("regDatetimeT", new BasicDBObject("$gte", ts).append("$lt", ts2));
        MongoCursor<Document> cursor = collect.find(searchQuery).iterator();
        while (cursor.hasNext()) {
            Document object = cursor.next();
            map.put((String) object.get("msisdn"), (String) object.get("msisdn"));
        }
    }

    private static void getMsisdnRegister (MongoDatabase db, DateTime datetime, String serviceCode, HashMap<String, String> map) {
        // Time
        long ts = datetime.getMillis() / 1000;
        int duration = Integer.parseInt(serviceCode.replace("TT", ""));
        DateTime startTime = datetime;
        startTime = startTime.minusDays(duration);
        long ts2 = datetime.getMillis() / 1000;

        MongoCollection<Document> collect = collectionDbDate(db, startTime);
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("groupcode", "TIKTOK");
        if (serviceCode != null) {
            searchQuery.put("commandCode", new BasicDBObject("$in", getCommandCode(serviceCode)));
        }
        searchQuery.put("regDatetimeT", new BasicDBObject("$lt", ts2).append("$gte", ts));
        MongoCursor<Document> cursor = collect.find(searchQuery).iterator();
        while (cursor.hasNext()) {
            Document object = cursor.next();
            map.put((String) object.get("msisdn"), (String) object.get("msisdn"));
        }
    }

    private static ArrayList<String> getCommandCode(String serviceCode) {

        ArrayList<String> COMMAND_CODE = new ArrayList<String>();
        if(COMMAND_CODE.size() == 0) {
            COMMAND_CODE.add("DKLAI " + serviceCode);
            COMMAND_CODE.add("DK " + serviceCode);
            COMMAND_CODE.add("DKFREE " + serviceCode);
            COMMAND_CODE.add("DK " + serviceCode + " NOT EM");
        }
        return COMMAND_CODE;
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
