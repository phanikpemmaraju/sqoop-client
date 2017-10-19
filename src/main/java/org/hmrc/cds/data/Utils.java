package org.hmrc.cds.data;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;


public class Utils {

    private final String LINE_SEPERATOR = System.lineSeparator();

    private final StandardOpenOption APPEND = StandardOpenOption.APPEND;

    private static final String HDFS_TMP_PATH = "/tmp/";

    String getSourceFilePath(String fileName) throws IOException {
        final File file = Paths.get(fileName).toFile();
        return file.getCanonicalPath();
    }

    String getHDFSFilePath(String fileName) {
        return HDFS_TMP_PATH + fileName;
    }

    void writeToFile(File file, List<String> hqlStatements) throws IOException {
        Iterator<String> sqlIterator = hqlStatements.iterator();
        while (sqlIterator.hasNext()) {
            Files.write(file.toPath(), (sqlIterator.next().trim() + LINE_SEPERATOR).getBytes(), APPEND);
        }
    }


}
