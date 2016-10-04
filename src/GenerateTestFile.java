import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Random;

/**
 * Created by manduodong on 10/3/16.
 *
 */
public class GenerateTestFile {
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
        generateTestFile.generateFile("test2", 10000000, 50000);
    }
}
