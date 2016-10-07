import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class PartitionTest {
    static private String FILE_NAME = "testPartition";
    static private int splitSize = 3;
    static private int lineSize = 10000;
    static private int LINE = 35000;
    static private int USER_RANGE = 500;

    @BeforeClass
    public static void setup() throws IOException {
        GenerateTestFile generateTestFile = new GenerateTestFile();
        generateTestFile.generateFile(FILE_NAME, LINE, USER_RANGE);
    }

    @Test
    public void testPartition() throws InterruptedException, IOException {
        File file;
        Partition partition = new Partition();
        partition.partitionFile(new File(String.format("data/%s.txt", FILE_NAME)), splitSize, lineSize);
        Thread.sleep(1500);

        for (int index = 0; index < splitSize; index++) {
            for (int cycle = 0; cycle < Math.ceil(LINE / (double) lineSize); cycle++) {
                file = new File(String.format("data/%s.txt.%d.%d.txt", FILE_NAME, index, cycle));
                assertTrue(file.exists());
            }
        }
    }

    //RUN after testPartition
    @Test
    public void testPartitionContent() throws InterruptedException, IOException {
        BufferedReader br;
        String line;
        long time = 0;

        for (int index = 0; index < splitSize; index++) {
            for (int cycle = 0; cycle < Math.ceil(LINE / (double) lineSize); cycle++) {
                br = new BufferedReader(new FileReader(new File(
                        String.format("data/%s.txt.%d.%d.txt", FILE_NAME, index, cycle))));
                while ((line = br.readLine()) != null) {
                    String[] userLog = line.split(",");
                    assertTrue(Integer.valueOf(userLog[0]) % splitSize == index);
                    assertTrue(Long.valueOf(userLog[1]) >= time);
                    time = Long.valueOf(userLog[1]);
                    assertTrue("open".equals(userLog[2]) || "close".equals(userLog[2]));
                }
            }
            time = 0;
        }
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(String.format("data/%s.txt", FILE_NAME));
        file.delete();

        for (int index = 0; index < splitSize; index++) {
            for (int cycle = 0; cycle < Math.ceil(LINE / (double) lineSize); cycle++) {
                file = new File(String.format("data/%s.txt.%d.%d.txt", FILE_NAME, index, cycle));
                file.delete();
            }
        }
    }
}
