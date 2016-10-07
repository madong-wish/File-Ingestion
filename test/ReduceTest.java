import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ReduceTest {
    static private HashMap<String, String> resultMap = new HashMap<String, String>();
    @BeforeClass
    public static void setup() {
        File file1 = new File("data/test_template.txt");
        File file2 = new File("data/testReduce.txt.0.0.txt");

        resultMap.put("0", "2.3333333333333335");
        resultMap.put("1", "6.0");
        resultMap.put("2", "1.0");
        resultMap.put("3", "7.0");
        resultMap.put("4", "25.0");
        resultMap.put("5", "2.0");
        resultMap.put("6", "5.75");
        resultMap.put("7", "1.0");
        resultMap.put("8", "0.5");
        resultMap.put("9", "0.5");

        try {
            FileChannel src = new FileInputStream(file1).getChannel();
            FileChannel destination1 = new FileOutputStream(file2).getChannel();
            destination1.transferFrom(src, 0, src.size());
            src.close();
            destination1.close();
        } catch (IOException e) {
            System.out.println(String.format("Test failed at Setup due to: %s", e.toString()));
        }
    }

    /*  FILE/POLICY                     STRICT      UPDATE
    *   3593,1475556571015,open         Read        Read
        3593,1475556571042,close        Read        Read
        3593,1475556571100,open         Read        Read
        3593,1475556571145,open         Ignored     Replace prev
        3593,1475556571175,close        Read        Read
        3593,1475556571200,open         Read        Read
        3593,1475556571215,open         Ignored     Replace prev
        3593,1475556571240,close        Read        Read
        3593,1475556571242,close        Ignored     Ignored
        3593,1475556571243,open         Read        Read
        3593,1475556571258,close        Read        Read

STRICT:(1475556571042-1475556571015+1475556571175-1475556571100+1475556571240-1475556571200+1475556571258-1475556571243)/4=39.25
UPDATE:(1475556571042-1475556571015+1475556571175-1475556571145+1475556571240-1475556571215+1475556571258-1475556571243)/4=24.25
    * */

    @Test
    public void testReduceStrict() throws InterruptedException, IOException {
        new Thread(new Reduce("testReduce.txt", 0, 0, "Strict", false)).start();
        Thread.sleep(1000);
        File file = new File("data/testReduce.txt.result.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String result = br.readLine();
        assertEquals(result, "{3593,39.25}");
        file.delete();
    }

    @Test
    public void testReduceUpdate() throws InterruptedException, IOException {
        new Thread(new Reduce("testReduce.txt", 0, 0, "Update", true)).start();
        Thread.sleep(1000);
        BufferedReader br = new BufferedReader(new FileReader(new File("data/testReduce.txt.result.txt")));
        String result = br.readLine();
        assertEquals(result, "{3593,24.25}");
    }

    @Test
    public void testReduceMultipleFiles() throws InterruptedException, IOException {
        new Thread(new Reduce("testReduceM.txt", 1, 0, "Update", false)).start();
        new Thread(new Reduce("testReduceM.txt", 1, 1, "Update", false)).start();
        new Thread(new Reduce("testReduceM.txt", 1, 2, "Update", false)).start();

        Thread.sleep(6000);
        BufferedReader br = new BufferedReader(new FileReader(new File("data/testReduceM.txt.result.txt")));
        String result;

        while ((result = br.readLine()) != null) {
            String[] kv = result.split(",");
            assertEquals(resultMap.get(kv[0].substring(1)) + "}", kv[1]);
        }
    }

    @AfterClass
    public static void tearDown() {
        String[] fileNames = {"testReduce", "testReduceM"};

        for (String name : fileNames) {
            File file = new File(String.format("data/%s.txt.result.txt", name));
            file.delete();
        }
        resultMap.clear();
    }


}

