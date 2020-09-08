package source.dataip;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import source.helper.ConnectDb;
import source.report.Menu;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ScanTiktokOld extends Thread {

    @Override
    public void run() {
        while (Menu.isRunning) {
            try {
                sleepTime();
//                process(null, null,  null);
//                process("TT1", null,  null);
//                process("TT7", null,  null);
//                process("TT30", null,  null);
//                process("TT80", null,  null);
//                process("TIKTOK90", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
//        process(null, fromDate,  toDate);
//        process("TT1", fromDate,  toDate);
//        process("TT7", fromDate,  toDate);
//        process("TT30", fromDate,  toDate);
//        process("TT80", fromDate,  toDate);
//        process("TIKTOK90", fromDate,  toDate);
        process("MF4IP", fromDate,  toDate);
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

            for(DateTime currentdate = toDate;
                currentdate.isAfter(fromDate) || currentdate.isEqual(fromDate);
                currentdate = currentdate.minusDays(1)) {
                System.out.println("TIKTOK_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage;
                // Tổng lượt đăng ký tất cả các gói
                listPackage = new String[]{
                        "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", "DKLAI TIKTOK90", "DKLAI MF4IP",
                        "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90", "DK TIKTOK90", "DK MF4IP",
                        "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", "DKFREE TIKTOK90", "DKFREE MF4IP",
                        "DK TT1 NOT EM", "DK TT7 NOT EM", "DK TT30 NOT EM", "DK TT80 NOT EM", "DK TIKTOK90 NOT EM", "DK MF4IP NOT EM",
                };
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

                // Tổng lượt đăng ký mới gọi MF4IP
                listPackage = new String[]{
                        "DK MF4IP"
                };
                int numberNewMF4IP = countRegister(database, currentdate, listPackage, packageFilter);

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
                int numberCancel = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90", "HUY MF4IP"
                };
                int numberCancelUser = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90", "HTHUY MF4IP",
                };
                int numberCancelSystem = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", "DKLAI TIKTOK90", "DKLAI MF4IP",
                        "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90", "DK MF4IP",
                        "GH TT1", "GH TT7", "GH TT30", "GH TT80", "GH TIKTOK90", "GH MF4IP",
                        "GHMK TIKTOK90", "GHMK MF4IP",
                        "GHCD TIKTOK90", "GHCD MF4IP"
                };
                int numberPSC = countRegister(database, currentdate, listPackage, packageFilter);

                // Tổng doanh thu
                int numberRevenue = sumRevenue(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký hủy trong ngày
                int numberInday = countInday(database, currentdate, packageFilter);

                // Tổng thuê bao đăng ký trong 30 ngày
                int number30Day = countRepeat30Day(database, currentdate, packageFilter);

                // Hủy do trừ cước 30 ngày không thành công
                int numberCancel30Day = 0;
                if (packageFilter == "TT1" || packageFilter == "TT7") {
                    numberCancel30Day = getNewCancel(database, currentdate, packageFilter);
                } else if (packageFilter == "TT30" || packageFilter == "TT80") {

                } else if (packageFilter == null) {
                    int numberCancel30DayTT1 = getNewCancel(database, currentdate, "TT1");
                    int numberCancel30DayTT7 = getNewCancel(database, currentdate, "TT7");
                    numberCancel30Day = numberCancel30DayTT1 + numberCancel30DayTT7;
                }

                // Hủy do không gia hạn
                int numberKGH = numberCancelSystem - numberCancel30Day;

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
                insertData.put("registerCancel30Day", numberCancel30Day);
                insertData.put("registerKGH", numberKGH);
                if (packageFilter != null) {
                    insertData.put("package", packageFilter);

                    if (packageFilter.equals("MF4IP")) {
                        int numberRevenueMF4IP = (numberNewMF4IP + numberAgainMF4IP + numberMore) * 3000;
                        insertData.put("registerRevenueMF4IP", numberRevenueMF4IP);
                    }
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
                    "DKLAI TT1", "DKLAI TT7", "DKLAI TT30", "DKLAI TT80", "DKLAI TIKTOK90", "DKLAI MF4IP",
                    "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90", "DK MF4IP",
                    "GH TT1", "GH TT7", "GH TT30", "GH TT80", "GH TIKTOK90", "GH MF4IP",
                    "GHMK TIKTOK90", "GHMK MF4IP",
                    "GHCD TIKTOK90", "GHCD MF4IP"
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
                    "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90", "HTHUY MF4IP",
                    "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90", "HUY MF4IP",
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
                    "DK TT1", "DK TT7", "DK TT30", "DK TT80", "DK TIKTOK90", "DK MF4IP",
                    "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80", "DKFREE TIKTOK90", "DKFREE MF4IP"
            };
            BasicDBObject match = null;
            if (packageFilter == null) {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "TIKTOK"
                        ).append("regDatetimeD",
                                new BasicDBObject("$lte", new Timestamp(datetime.plusDays(1).getMillis()))
                                        .append("$gte", new Timestamp(datetime.minusDays(30).getMillis()))
                        ).append("status", "3")
                );
            } else {
                match = new BasicDBObject("$match",
                        new BasicDBObject("groupcode", "TIKTOK"
                        ).append("regDatetimeD",
                                new BasicDBObject("$lte", new Timestamp(datetime.plusDays(1).getMillis()))
                                        .append("$gte", new Timestamp(datetime.minusDays(30).getMillis()))
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
            searchQueryData.put("regDatetimeD",
                    new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                            .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
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
            MongoCollection<Document> collection = database.getCollection("register");
            BasicDBObject searchQueryData = new BasicDBObject();
            searchQueryData.put("commandCode",
                    new BasicDBObject("$in", listPackage)
            );
            searchQueryData.put("regDatetimeD",
                    new BasicDBObject("$gte", new Timestamp(datetime.getMillis()))
                            .append("$lt", new Timestamp(datetime.plusDays(1).getMillis()))
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
            MongoCollection<Document> table = db.getCollection("register");
            BasicDBObject search = new BasicDBObject();
            search.put("msisdn", new BasicDBObject("$nin", msisdn));
            Timestamp ts1 = new Timestamp(datetime.getMillis());
            Timestamp ts2 = new Timestamp(datetime.plusDays(1).getMillis());
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
            Timestamp ts = new Timestamp(datetime.getMillis());
            MongoCollection<Document> collect = db.getCollection("register");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("groupcode", "TIKTOK");
            searchQuery.put("commandCode", "GH " + serviceCode);
            long duration = 20L;//serviceCode.equalsIgnoreCase("TT80") ? 85 : (serviceCode.equalsIgnoreCase("TT30") ? 35 : (serviceCode.equalsIgnoreCase("TT7") ? 10 : 5));
            searchQuery.put("regDatetimeD", new BasicDBObject("$gte", new Timestamp(ts.getTime() - duration * 86400000L)).append("$lt", ts));
            MongoCursor<Document> cursor = collect.find(searchQuery).iterator();
            while (cursor.hasNext()) {
                Document object = cursor.next();
                map.put((String) object.get("msisdn"), (String) object.get("msisdn"));
            }

            getAllMsisdnRegister(db, datetime, serviceCode, map);

            Iterable<String> it = map.keySet();
            for(String key : it) {
                output.add(key)
                ;
            }
            map.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    private static void getAllMsisdnRegister(MongoDatabase db, DateTime datetime, String serviceCode, HashMap<String, String> map) {
        try {
            Timestamp ts1 = new Timestamp(datetime.getMillis());
            long duration = Long.valueOf(serviceCode.replace("TT", ""));
            Timestamp ts = new Timestamp(ts1.getTime() - duration * 86400000L);
            MongoCollection<Document> collect = db.getCollection("register");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("groupcode", "TIKTOK");
            if (serviceCode != null) {
                searchQuery.put("commandCode", new BasicDBObject("$in", getCommandCode(serviceCode)));
            }
            searchQuery.put("regDatetimeD", new BasicDBObject("$lt", new Timestamp(ts.getTime() + 86400000L)).append("$gte", ts));
            MongoCursor<Document> cursor = collect.find(searchQuery).iterator();
            while (cursor.hasNext()) {
                Document object = cursor.next();
                map.put((String) object.get("msisdn"), (String) object.get("msisdn"));
            }
        } catch (Exception e) {
            e.printStackTrace();
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

}
