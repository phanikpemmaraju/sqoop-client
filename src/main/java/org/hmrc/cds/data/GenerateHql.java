package org.hmrc.cds.data;

import org.apache.commons.io.FilenameUtils;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class GenerateHql {

    public static String CONNECTION_STRING = PropertiesFileUtil.getProperty("CONNECTION_STRING");
    public static String DB_NAME = PropertiesFileUtil.getProperty("DB_NAME");
    public static String USERNAME = PropertiesFileUtil.getProperty("USERNAME");
    public static String PASSWORD = PropertiesFileUtil.getProperty("PASSWORD");
    public static String DRIVER = PropertiesFileUtil.getProperty("DRIVER");
    public static String HDFS_URL =  PropertiesFileUtil.getProperty("HDFS_URL");
    public static String HIVE_CONNECTION_URL =  PropertiesFileUtil.getProperty("HIVE_URL");
    public static String HIVE_USER =  PropertiesFileUtil.getProperty("HIVE_USER");
    public static String HIVE_PASSWORD =  PropertiesFileUtil.getProperty("HIVE_PASSWORD");
    public static String HIVE_DRIVER =  PropertiesFileUtil.getProperty("HIVE_DRIVER");

    public static final String SQL_FILE_EXTENSION = ".sql";
    public static final String HQL_FILE_EXTENSION = ".q";
    public static final String BASH_SCRIPT_FILE = "mwbtosql.sh";

    private static Logger logger = LoggerFactory.getLogger(GenerateHql.class);

    private SqoopClient sqoopClient;

    public void setSqoopClient(SqoopClient sqoopClient) {
        this.sqoopClient = sqoopClient;
    }


    public static void main(String... args) throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        logger.info(">>>> Start of generation of Hive HQL from MySql Workbench file <<<< ");
        GenerateHql sql = new GenerateHql();
        String RESOURCES_PATH = "/home/developer/sqoop-files/DataVault_MySQLWB.mwb";//"src/main/resources";
        sql.generateHqlFromMwb(RESOURCES_PATH);
    }

    public void generateHqlFromMwb(String resourcesPath) throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        String filePath = FilenameUtils.getFullPath(resourcesPath);
        String fileName = FilenameUtils.getName(resourcesPath);
        String sqlFileName = getFileNameByExt(fileName, SQL_FILE_EXTENSION);
        final String hqlFileName = getFileNameByExt(fileName, HQL_FILE_EXTENSION);

        ProcessBuilder builder = new ProcessBuilder("/bin/bash", BASH_SCRIPT_FILE, fileName, sqlFileName);
        builder.directory(new File(filePath));
        Process process = builder.start();
        int exitCode = process.waitFor();
        if (exitCode == 0)
            mysqlLoad(filePath, sqlFileName);

        logger.info(">>>> SQL generated successfully <<<<");

        sqoopClient = (sqoopClient == null) ? new SqoopClient() : sqoopClient;
        sqoopClient.runSqoop(filePath, hqlFileName);

        logger.info(">>>> End of generation of Hive HQL from MySql Workbench file <<<<");
    }

    private void mysqlLoad(final String filePath, final String sqlFileName) throws ClassNotFoundException, SQLException, IOException {

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath + sqlFileName))) {
            Connection connection = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD);

            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.runScript(bufferedReader);
        }
    }

    private String getFileNameByExt(String fileName, String ext) {
        return fileName.substring(0, fileName.lastIndexOf(".")) + ext;
    }

}
