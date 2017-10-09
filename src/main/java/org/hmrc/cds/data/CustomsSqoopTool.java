package org.hmrc.cds.data;

import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.hive.TableDefWriter;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.tool.BaseSqoopTool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;


public class CustomsSqoopTool extends BaseSqoopTool {

    public CustomsSqoopTool() {
        super("customs-tool");
    }

    @Override
    public int run(SqoopOptions sqoopOptions) {
        return 0;
    }

    public void generateDataVaultHQL(SqoopOptions options, String outputDirectory , String fileName) throws IOException{
        final JobData jobData = new JobData(options, this);
        this.manager = (new ConnFactory(options.getConf())).getManager(jobData);
        final List<String> tables = Arrays.asList(this.manager.listTables());
        File file = new File(outputDirectory +  fileName);
        if(file.exists()){
            file.delete();
        }
        file.createNewFile();
        for(int tableIndex=0;tableIndex<tables.size();tableIndex++){
            String table = tables.get(tableIndex);
            generateTableHQL(options, file, table);
        }
    }

    private void generateTableHQL(SqoopOptions options, File file, String table) throws IOException {
        try{
            TableDefWriter tableWriter = new TableDefWriter(options, this.manager, table, table, options.getConf(), true);
            String createTableStr = tableWriter.getCreateTableStmt() + ";\n";
            String dropTable = "DROP TABLE " + table + ";\n";
            writeToFile(file, createTableStr, dropTable);
        } catch (IOException io){
            io.printStackTrace();
        }
    }

    private void writeToFile(File file, String createTableStr, String dropTable) throws IOException {
        Files.write(file.toPath(),dropTable.getBytes(), StandardOpenOption.APPEND);
        Files.write(file.toPath(), createTableStr.getBytes(), StandardOpenOption.APPEND);
    }

}
