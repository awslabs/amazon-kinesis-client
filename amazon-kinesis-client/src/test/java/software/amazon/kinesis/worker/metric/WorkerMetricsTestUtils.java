package software.amazon.kinesis.worker.metric;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class WorkerMetricsTestUtils {

    public static void writeLineToFile(final File file, final String line) throws IOException {
        final FileOutputStream fileOutputStream = new FileOutputStream(file);
        final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        outputStreamWriter.write(line);
        outputStreamWriter.close();
        fileOutputStream.close();
    }
}
