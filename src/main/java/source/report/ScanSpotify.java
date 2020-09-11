package source.report;

import org.apache.commons.text.StrSubstitutor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import source.helper.DBProcess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class ScanSpotify extends Thread {

    private static Connection conn = null;
    private static String[] LIST_PACKAGE = new String[]{
            "SF1","SF7","SF30","SF80","THAGA15"
    };

    @Override
    public void run() {
        //conn = DBProcess.getConnection("vascms", "vascms");
        conn = DBProcess.getConnection("tiktok", "dataip082020");

        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("SF1", null,  null);
                process("SF7", null,  null);
                process("SF30", null,  null);
                process("SF80", null,  null);
                process("THAGA15", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        DBProcess.closeConnection(conn, null, null);
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
//        process(null, fromDate,  toDate);
//        process("SF1", fromDate,  toDate);
//        process("SF7", fromDate,  toDate);
//        process("SF30", fromDate,  toDate);
//        process("SF80", fromDate,  toDate);
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
                System.out.println("SPOTIFY_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage = null;

                boolean conditionPackage = packageFilter == null ||
                        packageFilter.equals("SF1") ||
                        packageFilter.equals("SF7") ||
                        packageFilter.equals("SF30") ||
                        packageFilter.equals("SF80");

                // Tổng lượt đăng ký tất cả các gói
                if (conditionPackage) {
                    listPackage = new String[]{
                            "DKLAI SF1", "DKLAI SF7", "DKLAI SF30", "DKLAI SF80",
                            "DK SF1", "DK SF7", "DK SF30", "DK SF80",
                            "DK SF1 NOT EM", "DK SF7 NOT EM", "DK SF30 NOT EM", "DK SF80 NOT EM"
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

                // Tổng lượt đăng ký mới gói SF1
                listPackage = new String[]{
                        "DK SF1"
                };
                int numberNewSF1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF7
                listPackage = new String[]{
                        "DK SF7"
                };
                int numberNewSF7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF30
                listPackage = new String[]{
                        "DK SF30"
                };
                int numberNewSF30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói SF80
                listPackage = new String[]{
                        "DK SF80"
                };
                int numberNewSF80 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói THAGA15
                listPackage = new String[]{
                        "DK THAGA15"
                };
                int numberNewTHAGA15 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF1
                listPackage = new String[]{
                        "DKLAI SF1"
                };
                int numberAgainSF1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF7
                listPackage = new String[]{
                        "DKLAI SF7"
                };
                int numberAgainSF7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF30
                listPackage = new String[]{
                        "DKLAI SF30"
                };
                int numberAgainSF30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói SF80
                listPackage = new String[]{
                        "DKLAI SF80"
                };
                int numberAgainSF80 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói THAGA15
                listPackage = new String[]{
                        "DKLAI THAGA15"
                };
                int numberAgainTHAGA15 = countRegister(currentdate, listPackage, packageFilter);

                listPackage = new String[]{
                        "DKFREE SF1", "DKFREE SF7", "DKFREE SF30", "DKFREE SF80", "DKFREE THAGA15",
                };
                int numberNewFree = countRegister(currentdate, listPackage, packageFilter);

                // Tổng đăng ký miễn phí
                listPackage = new String[]{
                        "DK SF1 NOT EM", "DK SF7 NOT EM", "DK SF30 NOT EM", "DK SF80 NOT EM", "DK THAGA15 NOT EM",
                };
                int numberNewFail = countRegister(currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH SF1", "GH SF7", "GH SF30", "GH SF80", "GH THAGA15"
                };
                int numberMore = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY SF1", "HTHUY SF7", "HTHUY SF30", "HTHUY SF80", "HTHUY THAGA15",
                        "HUY SF1", "HUY SF7", "HUY SF30", "HUY SF80", "HUY THAGA15"
                };
                int numberCancel = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY SF1", "HUY SF7", "HUY SF30", "HUY SF80", "HUY THAGA15"
                };
                int numberCancelUser = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY SF1", "HTHUY SF7", "HTHUY SF30", "HTHUY SF80", "HTHUY THAGA15",
                };
                int numberCancelSystem = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI SF1", "DKLAI SF7", "DKLAI SF30", "DKLAI SF80", "DKLAI THAGA15",
                        "DK SF1", "DK SF7", "DK SF30", "DK SF80", "DK THAGA15",
                        "GH SF1", "GH SF7", "GH SF30", "GH SF80", "GH THAGA15"
                };
                int numberPSC = countRegister(currentdate, listPackage, packageFilter);

                // Tổng doanh thu
                int numberRevenue = sumRevenue(currentdate, packageFilter);

                // Tổng thuê bao đăng ký hủy trong ngày
                int numberInday = countInday(currentdate, packageFilter);

                // Check Data exit
                boolean checkExist = checkDataExist(currentdate, packageFilter);

                if (!checkExist) {
                    // Save to DB
                    String sql = "insert into REPORT_SPOTIFY " +
                            "(PACKAGE_CODE, CREATE_AT, SUCCESS_SF1, SUCCESS_SF7, SUCCESS_SF30, SUCCESS_SF80, SUCCESS_THAGA15, " +
                            "AGAIN_SF1, AGAIN_SF7, AGAIN_SF30, AGAIN_SF80, AGAIN_THAGA15, " +
                            "NEW_FREE, NEW_FAILE, MORE, CANCEL_ALL, CANCEL_USER, " +
                            "CANCEL_SYSTEM, PSC, REVENUE, INDAY, LOG_ALL) " +
                            "VALUES (${PACKAGE}, TO_DATE('${CREATE_AT}', 'yyyy/mm/dd'), ${SUCCESS_SF1}, ${SUCCESS_SF7}, " +
                            "${SUCCESS_SF30}, ${SUCCESS_SF80}, ${SUCCESS_THAGA15}," +
                            "${AGAIN_SF1}, ${AGAIN_SF7}, ${AGAIN_SF30}, ${AGAIN_SF80}, ${AGAIN_THAGA15}, " +
                            "${NEW_FREE}, ${NEW_FAILE}, ${MORE}, ${CANCEL_ALL}," +
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
                    valuesMap.put("SUCCESS_SF1", numberNewSF1);
                    valuesMap.put("SUCCESS_SF7", numberNewSF7);
                    valuesMap.put("SUCCESS_SF30", numberNewSF30);
                    valuesMap.put("SUCCESS_SF80", numberNewSF80);
                    valuesMap.put("SUCCESS_THAGA15", numberNewTHAGA15);
                    valuesMap.put("AGAIN_SF1", numberAgainSF1);
                    valuesMap.put("AGAIN_SF7", numberAgainSF7);
                    valuesMap.put("AGAIN_SF30", numberAgainSF30);
                    valuesMap.put("AGAIN_SF80", numberAgainSF80);
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

                    String sql = "UPDATE REPORT_SPOTIFY " +
                            "SET SUCCESS_SF1 = ${SUCCESS_SF1}," +
                            "SUCCESS_SF7 = ${SUCCESS_SF7}," +
                            "SUCCESS_SF30 = ${SUCCESS_SF30}," +
                            "SUCCESS_SF80 = ${SUCCESS_SF80}," +
                            "SUCCESS_THAGA15 = ${SUCCESS_THAGA15}," +
                            "AGAIN_SF1 = ${AGAIN_SF1}," +
                            "AGAIN_SF7 = ${AGAIN_SF7}," +
                            "AGAIN_SF30 = ${AGAIN_SF30}," +
                            "AGAIN_SF80 = ${AGAIN_SF80}," +
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

                    //Info
                    valuesMap.put("SUCCESS_SF1", numberNewSF1);
                    valuesMap.put("SUCCESS_SF7", numberNewSF7);
                    valuesMap.put("SUCCESS_SF30", numberNewSF30);
                    valuesMap.put("SUCCESS_SF80", numberNewSF80);
                    valuesMap.put("SUCCESS_THAGA15", numberNewTHAGA15);
                    valuesMap.put("AGAIN_SF1", numberAgainSF1);
                    valuesMap.put("AGAIN_SF7", numberAgainSF7);
                    valuesMap.put("AGAIN_SF30", numberAgainSF30);
                    valuesMap.put("AGAIN_SF80", numberAgainSF80);
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
            String sql = "select count(*) as count_data from REPORT_SPOTIFY " +
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
                    packageFilter.equals("SF1") ||
                    packageFilter.equals("SF7") ||
                    packageFilter.equals("SF30")
            ) {
                listPackage = new String[]{
                        "DKLAI SF1", "DKLAI SF7", "DKLAI SF30", "DKLAI SF80",
                        "DK SF1", "DK SF7", "DK SF30", "DK SF80",
                        "GH SF1", "GH SF7", "GH SF30", "GH SF80"
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
                    "HTHUY SF1", "HTHUY SF7", "HTHUY SF30", "HTHUY SF80", "HTHUY THAGA15",
                    "HUY SF1", "HUY SF7", "HUY SF30", "HUY SF80", "HUY THAGA15"
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
