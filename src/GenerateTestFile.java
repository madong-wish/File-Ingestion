import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Random;

/**
 * Created by manduodong on 10/3/16.
 * class to generate test file
 */
public class GenerateTestFile {
    /**
     * Split file into several chunks
     * @param fileName name of the file to be generated
     * @param lines number of total lines. 400M lines is about 10 GB file
     * @param userRange range of userID to be randomly generated, from 0
     */
    public void generateFile(String fileName, long lines, int userRange) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
                String.format("data/%s%s", fileName, ".txt"))));
        long count = 0;
        Random random = new Random();
        while (++count < lines) {
            writer.append(String.format("%d,%d,%s", random.nextInt(userRange), Instant.now().toEpochMilli(),
                    random.nextInt() < 0 ? "open" : "close"));
            writer.newLine();
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        GenerateTestFile generateTestFile = new GenerateTestFile();
        /*
            10M lines = ~ 230MB
            200 M lines = ~ 5GB
         */
        generateTestFile.generateFile("testReduce", 100, 10);
    }
}
