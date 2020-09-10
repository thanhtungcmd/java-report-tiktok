package source.report;

import com.mongodb.client.MongoDatabase;
import org.apache.commons.text.StrSubstitutor;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import source.helper.DBProcess;

public class ScanTiktok extends Thread {

    private static Connection conn = null;
    private static String[] LIST_PACKAGE = new String[]{
            "TT1","TT7","TT30","TT80",
            "TIKTOK90","MF4IP","THAGA7","THAGA15",
    };

    @Override
    public void run() {
        //conn = DBProcess.getConnection("vascms", "vascms");
        conn = DBProcess.getConnection("tiktok", "dataip082020");

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
                process("THAGA7", null,  null);
                process("THAGA15", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        DBProcess.closeConnection(conn, null, null);
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("TT1", fromDate,  toDate);
        process("TT7", fromDate,  toDate);
        process("TT30", fromDate,  toDate);
        process("TT80", fromDate,  toDate);
        process("TIKTOK90", fromDate,  toDate);
        process("MF4IP", fromDate,  toDate);
        process("THAGA7", fromDate,  toDate);
        process("THAGA15", fromDate,  toDate);
    }

    private void sleepTime() {
        try {
            for(int i = 0; i < 300; i++) {
                if(Menu.isRunning) {
                    Thread.sleep(10000);
                } else {
                    break;
                }
            }
        } catch(Exception ignore) {}
    }

    public static void process(String packageFilter, DateTime fromDate, DateTime toDate) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            toDate = (toDate == null) ? new DateTime(DateTimeZone.getDefault()).withTimeAtStartOfDay() : toDate;
            fromDate = (fromDate == null) ? toDate.minusDays(3) : fromDate;

            for(DateTime currentdate = toDate;
                currentdate.isAfter(fromDate) || currentdate.isEqual(fromDate);
                currentdate = currentdate.minusDays(1)) {
                System.out.println("TIKTOK_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage = null;

                // Tổng DK tất cả các gói
                boolean conditionPackage = packageFilter == null ||
                        packageFilter.equals("TT1") ||
                        packageFilter.equals("TT7") ||
                        packageFilter.equals("TT30") ||
                        packageFilter.equals("TT80");
                if (conditionPackage) {
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
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DKLAI THAGA7",
                            "DK THAGA7",
                            "DKFREE THAGA7",
                            "DK THAGA7 NOT EM",
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "DKLAI THAGA15",
                            "DK THAGA15",
                            "DKFREE THAGA15",
                            "DK THAGA15 NOT EM",
                    };
                }

                int numberNewAll = countRegister(currentdate, listPackage, packageFilter);

                // Tổng DK gói TT1
                listPackage = new String[]{
                        "DK TT1"
                };
                int numberNewTT1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng DK gói TT7
                listPackage = new String[]{
                        "DK TT7"
                };
                int numberNewTT7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng DK gói TT30
                listPackage = new String[]{
                        "DK TT30"
                };
                int numberNewTT30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng DK gói TT80
                listPackage = new String[]{
                        "DK TT80"
                };
                int numberNewTT80 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi TIKTOK90
                listPackage = new String[]{
                        "DK TIKTOK90"
                };
                int numberNewTIKTOK90 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi MF4IP
                listPackage = new String[]{
                        "DK MF4IP"
                };
                int numberNewMF4IP = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi THAGA7
                listPackage = new String[]{
                        "DK THAGA7"
                };
                int numberNewTHAGA7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi THAGA15
                listPackage = new String[]{
                        "DK THAGA15"
                };
                int numberNewTHAGA15 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT1
                listPackage = new String[]{
                        "DKLAI TT1"
                };
                int numberAgainTT1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT7
                listPackage = new String[]{
                        "DKLAI TT7"
                };
                int numberAgainTT7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT30
                listPackage = new String[]{
                        "DKLAI TT30"
                };
                int numberAgainTT30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TT80
                listPackage = new String[]{
                        "DKLAI TT80"
                };
                int numberAgainTT80 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói TIKTOK90
                listPackage = new String[]{
                        "DKLAI TIKTOK90"
                };
                int numberAgainTIKTOK90 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói MF4IP
                listPackage = new String[]{
                        "DKLAI MF4IP"
                };
                int numberAgainMF4IP = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói THAGA7
                listPackage = new String[]{
                        "DKLAI THAGA7"
                };
                int numberAgainTHAGA7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói THAGA15
                listPackage = new String[]{
                        "DKLAI THAGA15"
                };
                int numberAgainTHAGA15 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                if (conditionPackage) {
                    listPackage = new String[]{
                            "DKFREE TT1", "DKFREE TT7", "DKFREE TT30", "DKFREE TT80"
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "DKFREE TIKTOK90",
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "DKFREE MF4IP",
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DKFREE THAGA7",
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "DKFREE THAGA15",
                    };
                }
                int numberNewFree = countRegister(currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí không thành công
                if (conditionPackage) {
                    listPackage = new String[]{
                            "DK TT1 NOT EM", "DK TT7 NOT EM", "DK TT30 NOT EM", "DK TT80 NOT EM",
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "DK TIKTOK90 NOT EM"
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "DK MF4IP NOT EM",
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DK THAGA7 NOT EM",
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "DK THAGA15 NOT EM",
                    };
                }
                int numberNewFail = countRegister(currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                if (conditionPackage) {
                    listPackage = new String[]{
                            "GH TT1", "GH TT7", "GH TT30", "GH TT80"
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "GH TIKTOK90",
                            "GHMK TIKTOK90",
                            "GHCD TIKTOK90"
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "GH MF4IP",
                            "GHMK MF4IP",
                            "GHCD MF4IP"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "GH THAGA7",
                            "GHMK THAGA7",
                            "GHCD THAGA7"
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "GH THAGA15",
                            "GHMK THAGA15",
                            "GHCD THAGA15"
                    };
                }
                int numberMore = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                if (conditionPackage) {
                    listPackage = new String[]{
                            "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80",
                            "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80"
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "HTHUY TIKTOK90",
                            "HUY TIKTOK90"
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "HTHUY MF4IP",
                            "HUY MF4IP"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "HTHUY THAGA7",
                            "HUY THAGA7"
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "HTHUY THAGA15",
                            "HUY THAGA15"
                    };
                }
                int numberCancel = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                if (conditionPackage) {
                    listPackage = new String[]{
                            "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80"
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "HUY TIKTOK90"
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "HUY MF4IP"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "HUY THAGA7"
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "HUY THAGA15"
                    };
                }
                int numberCancelUser = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                if (conditionPackage) {
                    listPackage = new String[]{
                            "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80",
                    };
                } else if (packageFilter.equals("TIKTOK90")) {
                    listPackage = new String[]{
                            "HTHUY TIKTOK90",
                    };
                } else if (packageFilter.equals("MF4IP")) {
                    listPackage = new String[]{
                            "HTHUY MF4IP",
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "HTHUY THAGA7",
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "HTHUY THAGA15",
                    };
                }
                int numberCancelSystem = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{};
                if (conditionPackage) {
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
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DKLAI THAGA7",
                            "DK THAGA7",
                            "GH THAGA7",
                            "GHMK THAGA7",
                            "GHCD THAGA7"
                    };
                } else if (packageFilter.equals("THAGA15")) {
                    listPackage = new String[]{
                            "DKLAI THAGA15",
                            "DK THAGA15",
                            "GH THAGA15",
                            "GHMK THAGA15",
                            "GHCD THAGA15"
                    };
                }
                int numberPSC = countRegister(currentdate, listPackage, packageFilter);

                // Tổng doanh thu
                int numberRevenue = sumRevenue(currentdate, packageFilter);

                // Tổng thuê bao đăng ký hủy trong ngày
                int numberInday = countInday(currentdate, packageFilter);

                // Check Data exit
                boolean checkExist = checkDataExist(currentdate, packageFilter);

                if (!checkExist) {
                    // Save to DB
                    String sql = "insert into REPORT_TIKTOK " +
                            "(PACKAGE_CODE, CREATE_AT, SUCCESS_TT1, SUCCESS_TT7, SUCCESS_TT30, SUCCESS_TT80," +
                            "SUCCESS_TIKTOK90, SUCCESS_MF4IP, SUCCESS_THAGA7, SUCCESS_THAGA15," +
                            "AGAIN_TT1, AGAIN_TT7, AGAIN_TT30, AGAIN_TT80, AGAIN_TIKTOK90, AGAIN_MF4IP," +
                            "AGAIN_THAGA7, AGAIN_THAGA15, NEW_FREE, NEW_FAILE, MORE, CANCEL_ALL, CANCEL_USER, " +
                            "CANCEL_SYSTEM, PSC, REVENUE, INDAY, LOG_ALL) " +
                            "VALUES (${PACKAGE}, TO_DATE('${CREATE_AT}', 'yyyy/mm/dd'), ${SUCCESS_TT1}, ${SUCCESS_TT7}, " +
                            "${SUCCESS_TT30}, ${SUCCESS_TT80}, " +
                            "${SUCCESS_TIKTOK90}, ${SUCCESS_MF4IP}, ${SUCCESS_THAGA7}, ${SUCCESS_THAGA15}," +
                            "${AGAIN_TT1}, ${AGAIN_TT7}, ${AGAIN_TT30}, ${AGAIN_TT80}, ${AGAIN_TIKTOK90}, ${AGAIN_MF4IP}, " +
                            "${AGAIN_THAGA7}, ${AGAIN_THAGA15}, ${NEW_FREE}, ${NEW_FAILE}, ${MORE}, ${CANCEL_ALL}," +
                            "${CANCEL_USER}, ${CANCEL_SYSTEM}, ${PSC}, ${REVENUE}, ${INDAY}, ${LOG_ALL})";
                    Map<String, Object> valuesMap = new HashMap<String, Object>();

                    // PACKAGE
                    if (packageFilter == null) {
                        valuesMap.put("PACKAGE", "'ALL'");
                    } else {
                        valuesMap.put("PACKAGE", "'" + packageFilter + "'");
                    }

                    //CREATE_AT
                    DateTimeFormatter createFmt = DateTimeFormat.forPattern("yyyy/MM/dd");
                    valuesMap.put("CREATE_AT", currentdate.toString(createFmt));

                    //Info
                    valuesMap.put("SUCCESS_TT1", numberNewTT1);
                    valuesMap.put("SUCCESS_TT7", numberNewTT7);
                    valuesMap.put("SUCCESS_TT30", numberNewTT30);
                    valuesMap.put("SUCCESS_TT80", numberNewTT80);
                    valuesMap.put("SUCCESS_TIKTOK90", numberNewTIKTOK90);
                    valuesMap.put("SUCCESS_MF4IP", numberNewMF4IP);
                    valuesMap.put("SUCCESS_THAGA7", numberNewTHAGA7);
                    valuesMap.put("SUCCESS_THAGA15", numberNewTHAGA15);
                    valuesMap.put("AGAIN_TT1", numberAgainTT1);
                    valuesMap.put("AGAIN_TT7", numberAgainTT7);
                    valuesMap.put("AGAIN_TT30", numberAgainTT30);
                    valuesMap.put("AGAIN_TT80", numberAgainTT80);
                    valuesMap.put("AGAIN_TIKTOK90", numberAgainTIKTOK90);
                    valuesMap.put("AGAIN_MF4IP", numberAgainMF4IP);
                    valuesMap.put("AGAIN_THAGA7", numberAgainTHAGA7);
                    valuesMap.put("AGAIN_THAGA15", numberAgainTHAGA15);
                    valuesMap.put("NEW_FREE", numberNewFree);
                    valuesMap.put("NEW_FAILE", numberNewFail);
                    valuesMap.put("MORE", numberMore);
                    valuesMap.put("CANCEL_ALL", numberCancel);
                    valuesMap.put("CANCEL_USER", numberCancelUser);
                    valuesMap.put("CANCEL_SYSTEM", numberCancelSystem);
                    valuesMap.put("PSC", numberPSC);
                    valuesMap.put("REVENUE", numberRevenue);
                    valuesMap.put("INDAY", numberInday);
                    valuesMap.put("LOG_ALL", numberNewAll);

                    StrSubstitutor sub = new StrSubstitutor(valuesMap);
                    sql = sub.replace(sql);

                    // Excute
                    ps = conn.prepareStatement(sql);
                    ps.execute();
                } else {

                    String sql = "UPDATE REPORT_TIKTOK " +
                        "SET SUCCESS_TT1 = ${SUCCESS_TT1}," +
                        "SUCCESS_TT7 = ${SUCCESS_TT7}," +
                        "SUCCESS_TT30 = ${SUCCESS_TT30}," +
                        "SUCCESS_TT80 = ${SUCCESS_TT80}," +
                        "SUCCESS_TIKTOK90 = ${SUCCESS_TIKTOK90}," +
                        "SUCCESS_MF4IP = ${SUCCESS_MF4IP}," +
                        "SUCCESS_THAGA7 = ${SUCCESS_THAGA7}," +
                        "SUCCESS_THAGA15 = ${SUCCESS_THAGA15}," +
                        "AGAIN_TT1 = ${AGAIN_TT1}," +
                        "AGAIN_TT7 = ${AGAIN_TT7}," +
                        "AGAIN_TT30 = ${AGAIN_TT30}," +
                        "AGAIN_TT80 = ${AGAIN_TT80}," +
                        "AGAIN_TIKTOK90 = ${AGAIN_TIKTOK90}," +
                        "AGAIN_MF4IP = ${AGAIN_MF4IP}," +
                        "AGAIN_THAGA7 = ${AGAIN_THAGA7}," +
                        "AGAIN_THAGA15 = ${AGAIN_THAGA15}," +
                        "NEW_FREE = ${NEW_FREE}," +
                        "NEW_FAILE = ${NEW_FAILE}," +
                        "MORE = ${MORE}," +
                        "CANCEL_ALL = ${CANCEL_ALL}," +
                        "CANCEL_USER = ${CANCEL_USER}," +
                        "CANCEL_SYSTEM = ${CANCEL_SYSTEM}," +
                        "PSC = ${PSC}," +
                        "REVENUE = ${REVENUE}," +
                        "INDAY = ${INDAY}," +
                        "LOG_ALL = ${LOG_ALL} " +
                        "WHERE to_char(CREATE_AT, 'yyyymmdd') = '${datetime}' and " +
                        "PACKAGE_CODE = ${packageFilter}";
                    Map<String, Object> valuesMap = new HashMap<String, Object>();

                    // packageFilter
                    if (packageFilter == null) {
                        valuesMap.put("packageFilter", "'ALL'");
                    } else {
                        valuesMap.put("packageFilter", "'" + packageFilter + "'");
                    }

                    // datetime
                    DateTimeFormatter createFmt = DateTimeFormat.forPattern("yyyyMMdd");
                    valuesMap.put("datetime", currentdate.toString(createFmt));

                    valuesMap.put("SUCCESS_TT1", numberNewTT1);
                    valuesMap.put("SUCCESS_TT7", numberNewTT7);
                    valuesMap.put("SUCCESS_TT30", numberNewTT30);
                    valuesMap.put("SUCCESS_TT80", numberNewTT80);
                    valuesMap.put("SUCCESS_TIKTOK90", numberNewTIKTOK90);
                    valuesMap.put("SUCCESS_MF4IP", numberNewMF4IP);
                    valuesMap.put("SUCCESS_THAGA7", numberNewTHAGA7);
                    valuesMap.put("SUCCESS_THAGA15", numberNewTHAGA15);
                    valuesMap.put("AGAIN_TT1", numberAgainTT1);
                    valuesMap.put("AGAIN_TT7", numberAgainTT7);
                    valuesMap.put("AGAIN_TT30", numberAgainTT30);
                    valuesMap.put("AGAIN_TT80", numberAgainTT80);
                    valuesMap.put("AGAIN_TIKTOK90", numberAgainTIKTOK90);
                    valuesMap.put("AGAIN_MF4IP", numberAgainMF4IP);
                    valuesMap.put("AGAIN_THAGA7", numberAgainTHAGA7);
                    valuesMap.put("AGAIN_THAGA15", numberAgainTHAGA15);
                    valuesMap.put("NEW_FREE", numberNewFree);
                    valuesMap.put("NEW_FAILE", numberNewFail);
                    valuesMap.put("MORE", numberMore);
                    valuesMap.put("CANCEL_ALL", numberCancel);
                    valuesMap.put("CANCEL_USER", numberCancelUser);
                    valuesMap.put("CANCEL_SYSTEM", numberCancelSystem);
                    valuesMap.put("PSC", numberPSC);
                    valuesMap.put("REVENUE", numberRevenue);
                    valuesMap.put("INDAY", numberInday);
                    valuesMap.put("LOG_ALL", numberNewAll);

                    StrSubstitutor sub = new StrSubstitutor(valuesMap);
                    sql = sub.replace(sql);

                    ps = conn.prepareStatement(sql);
                    ps.execute();
                }

                DBProcess.closeConnection(null, ps, rs);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBProcess.closeConnection(null, ps, rs);
        }
    }

    private static boolean checkDataExist(DateTime datetime, String packageFilter) {
        boolean output = false;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            // SQL
            String sql = "select count(*) as count_data from REPORT_TIKTOK " +
                    "where to_char(CREATE_AT, 'yyyymmdd') = '${datetime}' and " +
                    "PACKAGE_CODE = ${packageFilter}";
            Map<String, Object> valuesMap = new HashMap<String, Object>();

            // packageFilter
            if (packageFilter == null) {
                valuesMap.put("packageFilter", "'ALL'");
            } else {
                valuesMap.put("packageFilter", "'" + packageFilter + "'");
            }

            // datetime
            DateTimeFormatter createFmt = DateTimeFormat.forPattern("yyyyMMdd");
            valuesMap.put("datetime", datetime.toString(createFmt));

            StrSubstitutor sub = new StrSubstitutor(valuesMap);
            sql = sub.replace(sql);

            // Excute
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                int count = rs.getInt("count_data");
                if (count > 0) {
                    output = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBProcess.closeConnection(null, ps, rs);
        }

        return output;
    }

    private static int countRegister(DateTime datetime, String[] listPackage, String packageFilter) {
        int output = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {

            // SQL
            String sql = "select count(*) as count_data from LOG_REQUEST partition (log_request_p${partition}) " +
                    "where 1=1 and COMMAND_CODE in (${listPackage}) " +
                    "and PACKAGE_CODE in (${packageFilter}) " +
                    "and to_char(CREATED_AT, 'yyyymmdd') = '${datetime}'";
            Map<String, Object> valuesMap = new HashMap<String, Object>();

            // listPackage
            StringBuilder strPackageBuild = new StringBuilder();
            for (String item: listPackage) {
                strPackageBuild.append("'"+ item + "',");
            }
            String strPackage = strPackageBuild.substring(0, strPackageBuild.length() - 1);
            valuesMap.put("listPackage", strPackage);

            // packageFilter
            if (packageFilter == null) {
                StringBuilder strFilterBuild = new StringBuilder();
                for (String item: LIST_PACKAGE) {
                    strFilterBuild.append("'"+ item + "',");
                }
                String strFilter = strFilterBuild.substring(0, strFilterBuild.length() - 1);
                valuesMap.put("packageFilter", strFilter);
            } else {
                valuesMap.put("packageFilter", "'" + packageFilter + "'");
            }

            // partition
            DateTimeFormatter partitionFmt = DateTimeFormat.forPattern("yyyyMM");
            DateTime dateApply = new DateTime(2020, 8, 1, 0, 0);
            if (datetime.getMillis() < dateApply.getMillis()) {
                valuesMap.put("partition", "202008");
            } else {
                valuesMap.put("partition", datetime.toString(partitionFmt));
            }

            // datetime
            DateTimeFormatter createFmt = DateTimeFormat.forPattern("yyyyMMdd");
            valuesMap.put("datetime", datetime.toString(createFmt));

            StrSubstitutor sub = new StrSubstitutor(valuesMap);
            sql = sub.replace(sql);

            // Excute
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                int count = rs.getInt("count_data");
                output = count;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBProcess.closeConnection(null, ps, rs);
        }

        return output;
    }

    private static int sumRevenue(DateTime datetime, String packageFilter) {
        int output = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            // List Package
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
            } else if (packageFilter.equals("THAGA7")) {
                listPackage = new String[]{
                        "DKLAI THAGA7",
                        "DK THAGA7",
                        "GH THAGA7",
                        "GHMK THAGA7",
                        "GHCD THAGA7"
                };
            } else if (packageFilter.equals("THAGA15")) {
                listPackage = new String[]{
                        "DKLAI THAGA15",
                        "DK THAGA15",
                        "GH THAGA15",
                        "GHMK THAGA15",
                        "GHCD THAGA15"
                };
            }

            // SQL
            String sql = "select sum(CHARGE_PRICE) as count_data from LOG_REQUEST partition (log_request_p${partition}) " +
                    "where 1=1 and COMMAND_CODE in (${listPackage}) " +
                    "and PACKAGE_CODE in (${packageFilter}) " +
                    "and to_char(CREATED_AT, 'yyyymmdd') = '${datetime}'";
            Map<String, Object> valuesMap = new HashMap<String, Object>();

            // listPackage
            StringBuilder strPackageBuild = new StringBuilder();
            for (String item: listPackage) {
                strPackageBuild.append("'"+ item + "',");
            }
            String strPackage = strPackageBuild.substring(0, strPackageBuild.length() - 1);
            valuesMap.put("listPackage", strPackage);

            // packageFilter
            if (packageFilter == null) {
                StringBuilder strFilterBuild = new StringBuilder();
                for (String item: LIST_PACKAGE) {
                    strFilterBuild.append("'"+ item + "',");
                }
                String strFilter = strFilterBuild.substring(0, strFilterBuild.length() - 1);
                valuesMap.put("packageFilter", strFilter);
            } else {
                valuesMap.put("packageFilter", "'" + packageFilter + "'");
            }

            // partition
            DateTimeFormatter partitionFmt = DateTimeFormat.forPattern("yyyyMM");
            DateTime dateApply = new DateTime(2020, 8, 1, 0, 0);
            if (datetime.getMillis() < dateApply.getMillis()) {
                valuesMap.put("partition", "202008");
            } else {
                valuesMap.put("partition", datetime.toString(partitionFmt));
            }

            // datetime
            DateTimeFormatter createFmt = DateTimeFormat.forPattern("yyyyMMdd");
            valuesMap.put("datetime", datetime.toString(createFmt));

            StrSubstitutor sub = new StrSubstitutor(valuesMap);
            sql = sub.replace(sql);

            // Excute
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                int count = rs.getInt("count_data");
                output = count;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBProcess.closeConnection(null, ps, rs);
        }

        return output;
    }

    private static int countInday (DateTime datetime, String packageFilter) {
        int output = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            String[] listPackage = new String[]{
                    "HTHUY TT1", "HTHUY TT7", "HTHUY TT30", "HTHUY TT80", "HTHUY TIKTOK90", "HTHUY MF4IP",
                    "HUY TT1", "HUY TT7", "HUY TT30", "HUY TT80", "HUY TIKTOK90", "HUY MF4IP",
            };

            // SQL
            String sql = "select count(*) as count_data" +
                " from log_request partition (log_request_p${partition}) where " +
                " to_char(created_at,'yyyymmdd')='${datetime}' and command_code in (${listPackage})" +
                " and package_code in (${packageFilter}) and isdn in " +
                " (select isdn from log_request partition(log_request_p${partition})" +
                " where to_char(created_at,'yyyymmdd')='${datetime}' and command_code " +
                " like 'DK%' and package_code in (${packageFilter}) )";
            Map<String, Object> valuesMap = new HashMap<String, Object>();

            // listPackage
            StringBuilder strPackageBuild = new StringBuilder();
            for (String item: listPackage) {
                strPackageBuild.append("'"+ item + "',");
            }
            String strPackage = strPackageBuild.substring(0, strPackageBuild.length() - 1);

            // packageFilter
            valuesMap.put("listPackage", strPackage);
            if (packageFilter == null) {
                StringBuilder strFilterBuild = new StringBuilder();
                for (String item: LIST_PACKAGE) {
                    strFilterBuild.append("'"+ item + "',");
                }
                String strFilter = strFilterBuild.substring(0, strFilterBuild.length() - 1);
                valuesMap.put("packageFilter", strFilter);
            } else {
                valuesMap.put("packageFilter", "'" + packageFilter + "'");
            }

            // partition
            DateTimeFormatter partitionFmt = DateTimeFormat.forPattern("yyyyMM");
            DateTime dateApply = new DateTime(2020, 8, 1, 0, 0);
            if (datetime.getMillis() < dateApply.getMillis()) {
                valuesMap.put("partition", "202008");
            } else {
                valuesMap.put("partition", datetime.toString(partitionFmt));
            }

            // datetime
            DateTimeFormatter createFmt = DateTimeFormat.forPattern("yyyyMMdd");
            valuesMap.put("datetime", datetime.toString(createFmt));

            StrSubstitutor sub = new StrSubstitutor(valuesMap);
            sql = sub.replace(sql);

            // Excute
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                int count = rs.getInt("count_data");
                output = count;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBProcess.closeConnection(null, ps, rs);
        }

        return output;
    }

}
