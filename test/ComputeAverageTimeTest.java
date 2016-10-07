import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class ComputeAverageTimeTest {
    //System Test - This Test Will Take Relatively Long Time

    static private String FILE_NAME = "testSystem";
    static private int SPLIT_SIZE = 10;
    static private int LINE_SIZE = 2500000;
    static private int TOTAL_LINE = 10000000;
    static private int USER_RANGE = 5000;


    @BeforeClass
    public static void setup() throws IOException {
        System.out.println("######## Generating Test File ... #########");
        System.out.println(String.format("This may take roughly %d seconds", TOTAL_LINE / 750000));
        GenerateTestFile generateTestFile = new GenerateTestFile();
        generateTestFile.generateFile(FILE_NAME, TOTAL_LINE, USER_RANGE);
    }

    @Test
    public void systemTest() throws InterruptedException, IOException {
        FILE_NAME += ".txt";
        Partition fileUtil = new Partition();
        System.out.println("######## Partitioning and Mapping... #########");
        int cycle = fileUtil.partitionFile(new File("data/" + FILE_NAME), SPLIT_SIZE, LINE_SIZE);

        File file = new File(String.format("data/%s%s", FILE_NAME, ".result.txt"));
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
        }

        Thread.sleep(TOTAL_LINE / 1200000);

        //Reduce in threads
        //Policy can be Strict or Update - see Reduce for details
        System.out.println("############### Reducing... ###################");
        for (int i = 0; i < SPLIT_SIZE; i++) {
            new Thread(new Reduce(FILE_NAME, cycle, i, "Strict", true)).start();
        }

        System.out.println(String.format("Wait result for %d sec", SPLIT_SIZE * TOTAL_LINE / 7500000));
        Thread.sleep(SPLIT_SIZE * TOTAL_LINE / 8000);

        BufferedReader br = new BufferedReader(new FileReader(file));

        String line;
        int countLine = 0;
        double averageTime;
        String userID;
        HashSet<String> userSet = new HashSet<String>();

        while ((line = br.readLine()) != null) {
            countLine++;
            String[] result = line.substring(1, line.length()-1).split(",");
            userID = result[0];
            averageTime = Double.valueOf(result[1]);

            assertTrue(Integer.valueOf(userID) <= USER_RANGE );

            assertTrue(averageTime < TOTAL_LINE / 450000.0 && averageTime > 0);   // average time should be less than total time to generate file
            assertFalse(userSet.contains(userID));  //unique user ID
            userSet.add(userID);
        }
        assertTrue(countLine > USER_RANGE * 0.95);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(String.format("data/%s", FILE_NAME));
        file.delete();

        file = new File(String.format("data/%s", FILE_NAME + ".result.txt"));
        file.delete();
    }
}
