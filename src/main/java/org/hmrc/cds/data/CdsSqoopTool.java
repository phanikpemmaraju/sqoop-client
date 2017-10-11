package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;
import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.hive.TableDefWriter;
import org.apache.sqoop.tool.BaseSqoopTool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class CdsSqoopTool extends BaseSqoopTool {

    private TableDefWriter tableWriter;

    private ConnManager manager;

    private final String LINE_SEPERATOR = System.lineSeparator();

    private final StandardOpenOption APPEND = StandardOpenOption.APPEND;

    private final String STORE_TEXT_FILE = " STORED AS TEXTFILE;";

    public CdsSqoopTool() {
        super("customs-tool");
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


    public void generateDataVaultHQL(SqoopOptions options, String outputDirectory , String fileName) throws IOException{
        final List<String> tables = getAllTables(options);
        File file = Paths.get(outputDirectory + File.separator + fileName).toFile();

        if(Files.notExists(file.toPath())) {
            file.getParentFile().mkdirs();
            Files.createFile(file.toPath());
        }else{
            file.delete();
            file.createNewFile();
        }

        for(int tableIndex=0;tableIndex<tables.size();tableIndex++){
            List<String> statements = generateTableHQL(options, tables.get(tableIndex));
            writeToFile(file, statements);
        }
    }

    public List<String> getAllTables(SqoopOptions options) throws IOException{
        final JobData jobData = new JobData(options, this);
        manager = (manager == null) ? (new ConnFactory(options.getConf())).getManager(jobData) : manager;
        return Arrays.asList(manager.listTables());
    }

    public List<String> generateTableHQL(SqoopOptions options, String table) throws IOException {
        tableWriter = (tableWriter == null) ? new TableDefWriter(options, this.manager, table, table, options.getConf(), false) : tableWriter;
        String createTableStr = tableWriter.getCreateTableStmt().split("ROW FORMAT")[0].trim() + STORE_TEXT_FILE;
        String dropTable = "DROP TABLE IF EXISTS " + table + ";";
        List<String> hqlStatements = new ArrayList<>();

        hqlStatements.add(dropTable); hqlStatements.add(createTableStr); hqlStatements.add("");tableWriter = null;
        return hqlStatements;
    }

    private void writeToFile(File file, List<String> hqlStatements) throws IOException {
        Iterator<String> sqlIterator = hqlStatements.iterator();
        while(sqlIterator.hasNext()){
            Files.write(file.toPath(),(sqlIterator.next().trim() + LINE_SEPERATOR).getBytes(), APPEND);
        }
    }

}
