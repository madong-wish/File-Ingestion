import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by manduodong on 9/29/16.
 *
 */

public class ComputeAverageTime {
    private static final int FILE_SPLIT_SIZE_PER_CYCLE = 20;
    private static final long MAX_LINE_PER_CYCLE = 1000000 * FILE_SPLIT_SIZE_PER_CYCLE; // < 40% memory
    public static boolean wait = true;
    private static final String FILE_NAME = "test2.txt";

    public static void main(String[] args) throws InterruptedException {
        Partition fileUtil = new Partition();
        System.out.println("######## Partitioning and Mapping... #########");
        int cycle = fileUtil.partitionFile(new File("data/" + FILE_NAME), FILE_SPLIT_SIZE_PER_CYCLE, MAX_LINE_PER_CYCLE);
        File file = new File(String.format("data/%s%s", FILE_NAME, ".result.txt"));
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        do {
            TimeUnit.SECONDS.sleep(5);
        } while (wait);
        //Reduce in threads
        //Policy can be Strict or Update - see Reduce for details
        System.out.println("############### Reducing... ###################");
        for (int i = 0; i < FILE_SPLIT_SIZE_PER_CYCLE; i++) {
            new Thread(new Reduce(FILE_NAME, cycle, i, "Strict", true)).start();
        }
    }
}
