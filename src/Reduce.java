import java.io.*;
import java.util.HashMap;

/**
 * Created by manduo dong on 10/3/16.
 *
 *  Reduce open / close to a total sum of time, weight which will be used to calculate average time
 *  Reduce Policy:
 *      Strict: (Default) strict open-close matching, open after open or close after close will be ignored
 *      Update: Update open time, when another open occurred after initial open without a close in between
 */
public class Reduce implements Runnable {
    private int cycle;
    private Integer index;
    private String fileName;
    private String policy;
    private static boolean writeLock = false;

    //dictionary: key: UserID, value: [total_close_open_diff, weight, latest_open]

    Reduce(String fileName, int cycle, int index, String policy) {
        this.fileName = fileName;
        this.cycle = cycle;
        this.index = index;
        this.policy = policy;
    }

    @Override
    public void run() {
        HashMap<String, long[]> userMap = new HashMap<String, long[]>();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i <= cycle; i++) {
            try {
                File file = new File(String.format("data/%s%s%d%s%d%s", fileName, ".", index, ".", i, ".txt"));
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                String line;
                try {
                    while ((line = br.readLine()) != null) {
                        String[] userInfo = line.split(",");
                        String key = userInfo[0];

                        if (!userMap.containsKey(key)) {
                            if (userInfo[2].equals("open")) {
                                userMap.put(key, new long[] {0L, 0L, 0L});
                            }
                        } else {
                            long[] val = userMap.get(key);
                            if (userInfo[2].equals("open") && (policy.equals("Update") || val[2] == 0L)) {
                                userMap.put(key, new long[] {val[0], val[1], Long.parseLong(userInfo[1])});
                            } else if (userInfo[2].equals("close") && val[2] > 0) {
                                userMap.put(key, new long[] {val[0]+Long.parseLong(userInfo[1])-val[2], val[1]+1, 0L});
                            }
                        }
                    }
                    if (!file.delete()) {
                        System.out.println("FAILED to delete file");
                    }
                } catch (IOException e) {
                        e.printStackTrace();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        System.out.println(String.format("Finished reducing %d th partition in: %s sec", index, (System.currentTimeMillis() - startTime)/1000));
        try {
            synchronized (this) {
                if (!writeLock) {
                    System.out.println(String.format("Obtained write lock for index: %d", index));
                    writeLock = true;
                }
                else
                {
                    while (writeLock) {
                        Thread.sleep(5000L);
                        if (!writeLock) {
                            writeLock = true;
                            System.out.println(String.format("Obtained write lock for index: %d", index));
                            break;
                        }
                    }
                }
            }
            //File file = new File(String.format("data/%s%d%s", fileName, index, ".txt"));
            File file = new File(String.format("data/%s%s", fileName, ".result.txt"));
            BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));
            for (HashMap.Entry<String, long[]> entry : userMap.entrySet()) {
                long[] value =  entry.getValue();

                if (value[1] != 0) {
                    writer.append("{").append(entry.getKey()).append(",").append(String.valueOf(value[0] / (double) value[1])).append("}");
                    writer.newLine();
                }
            }
            writer.close();
        } catch (IOException | InterruptedException e) {
                e.printStackTrace();
        }
        finally {
            synchronized (this) {
                writeLock = false;
                System.out.println("Released write lock");
            }
        }
    }
}
