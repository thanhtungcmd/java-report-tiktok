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

public class ScanYoutube extends Thread {

    private static Connection conn = null;
    private static String[] LIST_PACKAGE = new String[]{
            "YT1", "YT30", "HDY"
    };

    @Override
    public void run() {
        //conn = DBProcess.getConnection("vascms", "vascms");
        conn = DBProcess.getConnection("youtube", "dataip082020");

        while (Menu.isRunning) {
            try {
                sleepTime();
                process(null, null,  null);
                process("YT1", null,  null);
                process("YT30", null,  null);
                process("HDY", null,  null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        DBProcess.closeConnection(conn, null, null);
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("YT1", fromDate,  toDate);
        process("YT30", fromDate,  toDate);
        process("HDY", fromDate,  toDate);
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
                System.out.println("YT_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage = null;

                // Tổng lượt đăng ký tất cả các gói
                listPackage = new String[]{
                        "DKLAI YT1", "DKLAI YT30", "DKLAI HDY",
                        "DK YT1", "DK YT30", "DK HDY",
                        "DK YT1 NOT EM", "DK YT30 NOT EM", "DK HDY NOT EM"
                };
                int numberNewAll = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói YT1
                listPackage = new String[]{
                        "DK YT1"
                };
                int numberNewYT1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói YT30
                listPackage = new String[]{
                        "DK YT30"
                };
                int numberNewYT30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói HDY
                listPackage = new String[]{
                        "DK HDY"
                };
                int numberNewHDY = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói YT1
                listPackage = new String[]{
                        "DKLAI YT1"
                };
                int numberAgainYT1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói YT30
                listPackage = new String[]{
                        "DKLAI YT30"
                };
                int numberAgainYT30 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói HDY
                listPackage = new String[]{
                        "DKLAI HDY"
                };
                int numberAgainHDY = countRegister(currentdate, listPackage, packageFilter);

                // Tổng đăng ký thất bại
                listPackage = new String[]{
                        "DK YT1 NOT EM", "DK YT30 NOT EM", "DK HDY NOT EM"
                };
                int numberNewFail = countRegister(currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH YT1", "GH YT30", "GH HDY",
                        "GHMK YT1", "GHMK YT30", "GHMK HDY",
                };
                int numberMore = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY YT1", "HTHUY YT30", "HTHUY HDY",
                        "HUY YT1", "HUY YT30", "HUY HDY"
                };
                int numberCancel = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY YT1", "HUY YT30", "HUY HDY",
                };
                int numberCancelUser = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY YT1", "HTHUY YT30", "HTHUY HDY",
                };
                int numberCancelSystem = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI YT1", "DKLAI YT30", "DKLAI HDY",
                        "DK YT1", "DK YT30", "DK HDY",
                        "GH YT1", "GH YT30", "GH HDY",
                        "GHMK YT1", "GHMK YT30", "GHMK HDY"
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
                    String sql = "insert into REPORT_YOUTUBE " +
                            "(PACKAGE_CODE, CREATE_AT, SUCCESS_YT1, SUCCESS_YT30, SUCCESS_HDY, " +
                            "AGAIN_YT1, AGAIN_YT30, AGAIN_HDY, " +
                            "NEW_FAILE, MORE, CANCEL_ALL, CANCEL_USER, " +
                            "CANCEL_SYSTEM, PSC, REVENUE, INDAY, LOG_ALL) " +
                            "VALUES (${PACKAGE}, TO_DATE('${CREATE_AT}', 'yyyy/mm/dd'), ${SUCCESS_YT1}, ${SUCCESS_YT30}, ${SUCCESS_HDY}, " +
                            "${AGAIN_YT1}, ${AGAIN_YT30}, ${AGAIN_HDY}, " +
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
                    valuesMap.put("SUCCESS_YT1", numberNewYT1);
                    valuesMap.put("SUCCESS_YT30", numberNewYT30);
                    valuesMap.put("SUCCESS_HDY", numberNewHDY);
                    valuesMap.put("AGAIN_YT1", numberAgainYT1);
                    valuesMap.put("AGAIN_YT30", numberAgainYT30);
                    valuesMap.put("AGAIN_HDY", numberAgainHDY);
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

                    String sql = "UPDATE REPORT_YOUTUBE " +
                            "SET SUCCESS_YT1 = ${SUCCESS_YT1}," +
                            "SUCCESS_YT30 = ${SUCCESS_YT30}," +
                            "SUCCESS_HDY = ${SUCCESS_HDY}," +
                            "AGAIN_YT1 = ${AGAIN_YT1}," +
                            "AGAIN_YT30 = ${AGAIN_YT30}," +
                            "AGAIN_HDY = ${AGAIN_HDY}," +
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
                    valuesMap.put("SUCCESS_YT1", numberNewYT1);
                    valuesMap.put("SUCCESS_YT30", numberNewYT30);
                    valuesMap.put("SUCCESS_HDY", numberNewHDY);
                    valuesMap.put("AGAIN_YT1", numberAgainYT1);
                    valuesMap.put("AGAIN_YT30", numberAgainYT30);
                    valuesMap.put("AGAIN_HDY", numberAgainHDY);
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
            String sql = "select count(*) as count_data from REPORT_YOUTUBE " +
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
            String[] listPackage = new String[]{
                    "DKLAI YT1", "DKLAI YT30", "DKLAI HDY",
                    "DK YT1", "DK YT30", "DK HDY",
                    "GH YT1", "GH YT30", "GH HDY",
                    "GHMK YT1", "GHMK YT30", "GHMK HDY",
            };

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
                    "HTHUY YT1", "HTHUY YT30", "HTHUY HDY",
                    "HUY YT1", "HUY YT30", "HTHUY HDY"
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
