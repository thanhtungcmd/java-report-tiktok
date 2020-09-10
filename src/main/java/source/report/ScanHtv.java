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

public class ScanHtv extends Thread {

    private static Connection conn = null;
    private static String[] LIST_PACKAGE = new String[]{
            "HTV1","HTV150"
    };

    @Override
    public void run() {
        //conn = DBProcess.getConnection("vascms", "vascms");
        conn = DBProcess.getConnection("htv", "dataip082020");

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

        DBProcess.closeConnection(conn, null, null);
    }

    public static void scan(DateTime fromDate, DateTime toDate) {
        process(null, fromDate,  toDate);
        process("HTV1", fromDate,  toDate);
        process("HTV150", fromDate,  toDate);
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
                System.out.println("HTV_" + packageFilter + "_" + currentdate.toString());

                String[] listPackage = null;

                // Tổng lượt đăng ký tất cả các gói
                listPackage = new String[]{
                        "DKLAI HTV1", "DKLAI HTV150",
                        "DK HTV1", "DK HTV150",
                        "DK HTV1 NOT EM", "DK HTV150 NOT EM"
                };
                int numberNewAll = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói HTV1
                listPackage = new String[]{
                        "DK HTV1"
                };
                int numberNewHTV1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký mới gói HTV150
                listPackage = new String[]{
                        "DK HTV150"
                };
                int numberNewHTV150 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói HTV1
                listPackage = new String[]{
                        "DKLAI HTV1"
                };
                int numberAgainHTV1 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng lượt đăng ký lại gói HTV150
                listPackage = new String[]{
                        "DKLAI HTV150"
                };
                int numberAgainHTV150 = countRegister(currentdate, listPackage, packageFilter);

                // Tổng đăng ký thất bại
                listPackage = new String[]{
                        "DK HTV1 NOT EM", "DK HTV150 NOT EM"
                };
                int numberNewFail = countRegister(currentdate, listPackage, packageFilter);

                // Tổng gia hạn
                listPackage = new String[]{
                        "GH HTV1", "GH HTV150", "GHMK HTV1", "GHMK HTV150"
                };
                int numberMore = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy
                listPackage = new String[]{
                        "HTHUY HTV1", "HTHUY HTV150",
                        "HUY HTV1", "HUY HTV150"
                };
                int numberCancel = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do người dùng hủy
                listPackage = new String[]{
                        "HUY HTV1", "HUY HTV150"
                };
                int numberCancelUser = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb hủy do hệ thống hủy
                listPackage = new String[]{
                        "HTHUY HTV1", "HTHUY HTV150",
                };
                int numberCancelSystem = countRegister(currentdate, listPackage, packageFilter);

                // Tổng tb phát sinh cước
                listPackage = new String[]{
                        "DKLAI HTV1", "DKLAI HTV150",
                        "DK HTV1", "DK HTV150",
                        "GH HTV1", "GH HTV150",
                        "GHMK HTV1", "GHMK HTV150"
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
                            "(PACKAGE_CODE, CREATE_AT, SUCCESS_HTV1, SUCCESS_HTV150, " +
                            "AGAIN_HTV1, AGAIN_HTV150, " +
                            "NEW_FAILE, MORE, CANCEL_ALL, CANCEL_USER, " +
                            "CANCEL_SYSTEM, PSC, REVENUE, INDAY, LOG_ALL) " +
                            "VALUES (${PACKAGE}, TO_DATE('${CREATE_AT}', 'yyyy/mm/dd'), ${SUCCESS_HTV1}, ${SUCCESS_HTV150}, " +
                            "${AGAIN_HTV1}, ${AGAIN_HTV150}, " +
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
                    valuesMap.put("SUCCESS_HTV1", numberNewHTV1);
                    valuesMap.put("SUCCESS_HTV150", numberNewHTV150);
                    valuesMap.put("AGAIN_HTV1", numberAgainHTV1);
                    valuesMap.put("AGAIN_HTV150", numberAgainHTV150);
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

                    String sql = "UPDATE REPORT_HTV " +
                            "SET SUCCESS_HTV1 = ${SUCCESS_HTV1}," +
                            "SUCCESS_HTV150 = ${SUCCESS_HTV150}," +
                            "AGAIN_HTV1 = ${AGAIN_HTV1}," +
                            "AGAIN_HTV150 = ${AGAIN_HTV150}," +
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
                    valuesMap.put("SUCCESS_HTV1", numberNewHTV1);
                    valuesMap.put("SUCCESS_HTV150", numberNewHTV150);
                    valuesMap.put("AGAIN_HTV1", numberAgainHTV1);
                    valuesMap.put("AGAIN_HTV150", numberAgainHTV150);
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
            String sql = "select count(*) as count_data from REPORT_HTV " +
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
                    "DKLAI HTV1", "DKLAI HTV150",
                    "DK HTV1", "DK HTV150",
                    "GH HTV1", "GH HTV150",
                    "GHMK HTV1", "GHMK HTV150"
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
                    "HTHUY HTV1", "HTHUY HTV150",
                    "HUY HTV1", "HUY HTV150",
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
