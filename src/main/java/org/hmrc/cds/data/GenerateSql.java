package org.hmrc.cds.data;

import org.apache.commons.io.FilenameUtils;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GenerateSql {

    public static final String CONNECTION_STRING = PropertiesFileUtil.getProperty("CONNECTION_STRING");
    public static final String DB_NAME = PropertiesFileUtil.getProperty("DB_NAME");
    public static final String USERNAME = PropertiesFileUtil.getProperty("USERNAME");
    public static final String PASSWORD = PropertiesFileUtil.getProperty("PASSWORD");
    public static final String DRIVER = PropertiesFileUtil.getProperty("DRIVER");

    private static Logger LOG = LoggerFactory.getLogger(GenerateSql.class);

    public static void main(String... args) throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        LOG.info(">>>> Start of generation of Hive HQL from MySql Workbench file <<<< ");
        GenerateSql sql = new GenerateSql();
        String RESOURCES_PATH = "/home/developer/sqoop-files/DataVault_MySQLWB.mwb";//"src/main/resources";
        sql.generateHqlFromMwb(RESOURCES_PATH);
    }

    public void generateHqlFromMwb(String resourcesPath) throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        String filePath = FilenameUtils.getFullPath(resourcesPath);
        String fileName = FilenameUtils.getName(resourcesPath);
        String sqlFileName = getFileNameByExt(fileName, ".sql");
        final String hqlFileName = getFileNameByExt(fileName, ".q");

        ProcessBuilder builder = new ProcessBuilder("/bin/bash", "mwbtosql.sh", fileName, sqlFileName);
        builder.directory(new File(filePath));
        Process process = builder.start();
        int exitCode = process.waitFor();
        if (exitCode == 0)
            mysqlLoad(filePath, sqlFileName);

        LOG.info(">>>> SQL generated successfully <<<<");

        new SqoopClient().runSqoop(filePath, hqlFileName);
        LOG.info(">>>> End of generation of Hive HQL from MySql Workbench file <<<<");
    }

    private void mysqlLoad(final String filePath, final String sqlFileName) throws ClassNotFoundException, SQLException, IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(new File(filePath + sqlFileName)));
             Connection connection = DriverManager.getConnection(CONNECTION_STRING,USERNAME,PASSWORD);) {

            ScriptRunner sr = new ScriptRunner(connection);
            sr.runScript(inputStreamReader);
        }
    }

    private String getFileNameByExt(String fileName, String ext) {
        return fileName.substring(0, fileName.lastIndexOf(".")) + ext;
    }

}
