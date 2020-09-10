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

public class ScanThvl extends Thread {

    private static Connection conn = null;
    private static String[] LIST_PACKAGE = new String[]{
            "VL1","VL30","VL80","THAGA7",
    };

    @Override
    public void run() {
        //conn = DBProcess.getConnection("vascms", "vascms");
        conn = DBProcess.getConnection("thvl", "dataip082020");

        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("VL1", null,  null);
                process("VL30", null,  null);
                process("VL80", null,  null);
                process("THAGA7", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        DBProcess.closeConnection(conn, null, null);
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("VL1", fromDate,  toDate);
        process("VL30", fromDate,  toDate);
        process("VL80", fromDate,  toDate);
        process("THAGA7", fromDate,  toDate);
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
                System.out.println("THVL_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage = null;

                boolean conditionPackage = packageFilter == null ||
                        packageFilter.equals("VL1") ||
                        packageFilter.equals("VL30") ||
                        packageFilter.equals("VL80");

                // Tổng lượt đăng ký tất cả các gói
                if (conditionPackage) {
                    listPackage = new String[]{
                            "DKLAI VL1", "DKLAI VL30", "DKLAI VL80",
                            "DK VL1", "DK VL30", "DK VL80",
                            "DK VL1 NOT EM", "DK VL30 NOT EM", "DK VL80 NOT EM"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DKLAI THAGA7",
                            "DK THAGA7",
                            "DKFREE THAGA7",
                            "DK THAGA7 NOT EM",
                    };
                }
                int numberNewAll = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói VL1
                listPackage = new String[]{
                        "DK VL1"
                };
                int numberNewVL1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói VL30
                listPackage = new String[]{
                        "DK VL30"
                };
                int numberNewVL30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói VL80
                listPackage = new String[]{
                        "DK VL80"
                };
                int numberNewVL80 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gọi THAGA7
                listPackage = new String[]{
                        "DK THAGA7"
                };
                int numberNewTHAGA7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói VL1
                listPackage = new String[]{
                        "DKLAI VL1"
                };
                int numberAgainVL1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói VL30
                listPackage = new String[]{
                        "DKLAI VL30"
                };
                int numberAgainVL30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói VL80
                listPackage = new String[]{
                        "DKLAI VL80"
                };
                int numberAgainVL80 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói THAGA7
                listPackage = new String[]{
                        "DKLAI THAGA7"
                };
                int numberAgainTHAGA7 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng đăng ký thất bại
                if (conditionPackage) {
                    listPackage = new String[]{
                            "DK VL1 NOT EM", "DK VL30 NOT EM", "DK VL80 NOT EM"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DK THAGA7 NOT EM",
                    };
                }
                int numberNewFail = countRegister(currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                if (conditionPackage) {
                    listPackage = new String[]{
                            "GH VL1", "GH VL30", "GH VL80",
                            "GHMK VL1", "GHMK VL30", "GHMK VL80"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "GH THAGA7",
                            "GHMK THAGA7",
                            "GHCD THAGA7"
                    };
                }
                int numberMore = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                if (conditionPackage) {
                    listPackage = new String[]{
                            "HTHUY VL1", "HTHUY VL30", "HTHUY VL80",
                            "HUY VL1", "HUY VL30", "HUY VL80"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "HTHUY THAGA7",
                            "HUY THAGA7"
                    };
                }
                int numberCancel = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                if (conditionPackage) {
                    listPackage = new String[]{
                            "HUY VL1", "HUY VL80", "HUY VL80"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "HUY THAGA7"
                    };
                }
                int numberCancelUser = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                if (conditionPackage) {
                    listPackage = new String[]{
                            "HTHUY VL1", "HTHUY VL30", "HTHUY VL80"
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "HTHUY THAGA7",
                    };
                }
                int numberCancelSystem = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                if (conditionPackage) {
                    listPackage = new String[]{
                            "DKLAI VL1", "DKLAI VL30", "DKLAI VL80",
                            "DK VL1", "DK VL30", "DK VL80",
                            "GH VL1", "GH VL30", "GH VL80",
                            "GHMK VL1", "GHMK VL30", "GHMK VL80",
                    };
                } else if (packageFilter.equals("THAGA7")) {
                    listPackage = new String[]{
                            "DKLAI THAGA7",
                            "DK THAGA7",
                            "GH THAGA7",
                            "GHMK THAGA7",
                            "GHCD THAGA7"
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
                    String sql = "insert into REPORT_THVL " +
                            "(PACKAGE_CODE, CREATE_AT, SUCCESS_VL1, SUCCESS_VL30, SUCCESS_VL80, SUCCESS_THAGA7," +
                            "AGAIN_VL1, AGAIN_VL30, AGAIN_VL80, AGAIN_THAGA7," +
                            "NEW_FAILE, MORE, CANCEL_ALL, CANCEL_USER, " +
                            "CANCEL_SYSTEM, PSC, REVENUE, INDAY, LOG_ALL) " +
                            "VALUES (${PACKAGE}, TO_DATE('${CREATE_AT}', 'yyyy/mm/dd'), ${SUCCESS_VL1}, ${SUCCESS_VL30}, " +
                            "${SUCCESS_VL80}, ${SUCCESS_THAGA7}, " +
                            "${AGAIN_VL1}, ${AGAIN_VL30}, ${AGAIN_VL80}, ${AGAIN_THAGA7}, " +
                            "${NEW_FAILE}, ${MORE}, ${CANCEL_ALL}," +
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
                    valuesMap.put("SUCCESS_VL1", numberNewVL1);
                    valuesMap.put("SUCCESS_VL30", numberNewVL30);
                    valuesMap.put("SUCCESS_VL80", numberNewVL80);
                    valuesMap.put("SUCCESS_THAGA7", numberNewTHAGA7);
                    valuesMap.put("AGAIN_VL1", numberAgainVL1);
                    valuesMap.put("AGAIN_VL30", numberAgainVL30);
                    valuesMap.put("AGAIN_VL80", numberAgainVL80);
                    valuesMap.put("AGAIN_THAGA7", numberAgainTHAGA7);
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

                    String sql = "UPDATE REPORT_THVL " +
                            "SET SUCCESS_VL1 = ${SUCCESS_VL1}," +
                            "SUCCESS_VL30 = ${SUCCESS_VL30}," +
                            "SUCCESS_VL80 = ${SUCCESS_VL80}," +
                            "SUCCESS_THAGA7 = ${SUCCESS_THAGA7}," +
                            "AGAIN_VL1 = ${AGAIN_VL1}," +
                            "AGAIN_VL30 = ${AGAIN_VL30}," +
                            "AGAIN_VL80 = ${AGAIN_VL80}," +
                            "AGAIN_THAGA7 = ${AGAIN_THAGA7}," +
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
                    valuesMap.put("SUCCESS_VL1", numberNewVL1);
                    valuesMap.put("SUCCESS_VL30", numberNewVL30);
                    valuesMap.put("SUCCESS_VL80", numberNewVL80);
                    valuesMap.put("SUCCESS_THAGA7", numberNewTHAGA7);
                    valuesMap.put("AGAIN_VL1", numberAgainVL1);
                    valuesMap.put("AGAIN_VL30", numberAgainVL30);
                    valuesMap.put("AGAIN_VL80", numberAgainVL80);
                    valuesMap.put("AGAIN_THAGA7", numberAgainTHAGA7);
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
            String sql = "select count(*) as count_data from REPORT_THVL " +
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
                    packageFilter.equals("VL1") ||
                    packageFilter.equals("VL30") ||
                    packageFilter.equals("VL80")
            ) {
                listPackage = new String[]{
                        "DKLAI VL1", "DKLAI VL30", "DKLAI VL80",
                        "DK VL1", "DK VL30", "DK VL80",
                        "GH VL1", "GH VL30", "GH VL80",
                        "GHMK VL1", "GHMK VL30", "GHMK VL80"
                };
            } else if (packageFilter.equals("THAGA7")) {
                listPackage = new String[]{
                        "DKLAI THAGA7",
                        "DK THAGA7",
                        "GH THAGA7",
                        "GHMK THAGA7",
                        "GHCD THAGA7"
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
                    "HTHUY VL1", "HTHUY VL30", "HTHUY VL80", "HTHUY THAGA7",
                    "HUY VL1", "HUY VL30", "HUY VL80", "HTHUY THAGA7",
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
