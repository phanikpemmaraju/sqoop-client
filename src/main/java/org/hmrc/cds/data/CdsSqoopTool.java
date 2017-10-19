package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.hive.TableDefWriter;
import org.apache.sqoop.tool.BaseSqoopTool;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang.StringUtils.EMPTY;

public class CdsSqoopTool extends BaseSqoopTool {

    private TableDefWriter tableWriter;

    private ConnManager manager;

    private Utils utils;

    private final String STORE_TEXT_FILE = " STORED AS TEXTFILE;";

    private final String TEST_FILES_PATH = "src/main/resources/test-files";

    public CdsSqoopTool() {
        super("customs-tool");
        utils = new Utils();
    }

    public void setTableWriter(TableDefWriter tableWriter) {
        this.tableWriter = tableWriter;
    }

    public void setManager(ConnManager manager) {
        this.manager = manager;
    }

    @Override
    public int run(SqoopOptions sqoopOptions) {
        return 1;
    }

    public void generateDataVaultHQL(SqoopOptions options, String outputDirectory, String fileName) throws IOException, SQLException, ClassNotFoundException {
        final List<String> tables = getAllTables(options);
        File file = Paths.get(outputDirectory + File.separator + fileName).toFile();

        if (Files.notExists(file.toPath())) {
            file.getParentFile().mkdirs();
            Files.createFile(file.toPath());
        } else {
            file.delete();
            file.createNewFile();
        }

        for (int tableIndex = 0; tableIndex < tables.size(); tableIndex++) {
            List<String> statements = generateTableHQL(options, tables.get(tableIndex));
            utils.writeToFile(file, statements);
        }
    }

    public void createAndPopulateDataVault() throws IOException, SQLException, ClassNotFoundException {
        File dataVaultFiles = Paths.get(TEST_FILES_PATH).toFile();
        List<String> files = Arrays.asList(dataVaultFiles.list());

        for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
            String filePath = files.get(fileIndex);
            writeToHDFSFile(filePath);
            writeToHiveTable(filePath);
        }
    }

    public List<String> getAllTables(SqoopOptions options) throws IOException {
        final JobData jobData = new JobData(options, this);
        manager = (manager == null) ? (new ConnFactory(options.getConf())).getManager(jobData) : manager;
        return Arrays.asList(manager.listTables());
    }

    public List<String> generateTableHQL(SqoopOptions options, String table) throws IOException {
        tableWriter = (tableWriter == null) ? new TableDefWriter(options, this.manager, table, table, options.getConf(), false) : tableWriter;
        String createTableStr = tableWriter.getCreateTableStmt().split("ROW FORMAT")[0].trim() + STORE_TEXT_FILE;
        String dropTable = "DROP TABLE IF EXISTS `" + options.getHiveDatabaseName() + "`." + table + ";";
        List<String> hqlStatements = new ArrayList<>();

        hqlStatements.add(dropTable);
        hqlStatements.add(createTableStr);
        hqlStatements.add("");
        tableWriter = null;
        return hqlStatements;
    }

    private void writeToHDFSFile(String fileName) throws IOException {
        final String sourceFilePath = utils.getSourceFilePath(TEST_FILES_PATH + File.separator + fileName);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(GenerateHql.HDFS_URL), conf);
        InputStream in = new BufferedInputStream(new FileInputStream(sourceFilePath));
        OutputStream out = fs.create(new Path(utils.getHDFSFilePath(fileName)), new Progressable() {
                    public void progress() {}});
        IOUtils.copyBytes(in, out, 4096, true);
    }

    private void writeToHiveTable(String tableName) throws SQLException, IOException, ClassNotFoundException {
        Class.forName(GenerateHql.HIVE_DRIVER);
        Connection con = DriverManager.getConnection(GenerateHql.HIVE_CONNECTION_URL, EMPTY, EMPTY);
        Statement stmt = con.createStatement();
        String sql = "load data inpath '" + utils.getHDFSFilePath(tableName) + "' overwrite into table " + tableName;
        stmt.execute(sql);
    }

}
