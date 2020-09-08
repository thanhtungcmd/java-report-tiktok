package source.report;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Menu extends Thread {

    public static boolean isRunning = true;
    private static final BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));

    public Menu() {
        try {
            // Scan Tiktok
            ScanTiktok scanTiktok = new ScanTiktok();
            scanTiktok.start();
            System.out.println("Scan started!!!");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void run() {
        executeMenu();
    }

    public void executeMenu() {
        while (isRunning) {
            String option = null;
            System.out.println();
            System.out.println("- Chon dich vu data ip ");
            System.out.println("- 1: Tiktok");
            System.out.print("> ");

            try {
                option = keyboard.readLine();
            } catch (Exception e) {
                System.out.println("exception reading keyboard " + e);
            }

            if ( option != null && option.length() == 1 ) {
                int scan = Integer.parseInt(option);

                if (scan == 1) {
                    executeTiktok();
                }
            }

        }
    }

    public void executeTiktok() {
        String option = null;
        System.out.println();
        System.out.println("- scan <day>");
        System.out.println("- yyyy mm dd");
        System.out.println("- Q Quit");
        System.out.print("> ");

        try {
            option = keyboard.readLine();
        } catch (Exception e) {
            System.out.println("exception reading keyboard " + e);
        }

        if ( option != null && option.startsWith("scan") ) {
            String[] split = option.split(" ");
            int scan = Integer.parseInt(split[1]);
            try {
                if (scan > 300) {
                    scan = 300;
                }
                DateTime toDate = new DateTime(DateTimeZone.getDefault()).withTimeAtStartOfDay();
                DateTime fromDate = toDate.minusDays(scan);

                ScanTiktok.scan(fromDate, toDate);
            } catch (Exception ignored) {}
        } else if (option != null && option.length() == 10) {
            try {
                String[] split = option.split(" ");
                int year = Integer.parseInt(split[0]);
                int month = Integer.parseInt(split[1]);
                int day = Integer.parseInt(split[2]);
                DateTime fromDate = new DateTime(year, month, day, 0, 0);
                ScanTiktok.scan(fromDate, fromDate);
            } catch (Exception ignore) {}
        } else if (option != null && "Q".equals(option.toUpperCase()) ) {
            exitMenu();
        }
    }

    public void exitMenu() {
        isRunning = false;
        try {
            System.out.print("Close resource & Exiting...");
            for (int i = 0; i < 5; i++) {
                System.out.print(".");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
