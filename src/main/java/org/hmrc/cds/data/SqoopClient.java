package org.hmrc.cds.data;

import com.cloudera.sqoop.SqoopOptions;

import java.io.IOException;


public class SqoopClient {

    private static SqoopOptions SqoopOptions = new SqoopOptions();
    private static final String CONNECTION_STRING = PropertiesFileUtil.getProperty("CONNECTION_STRING");
    private static final String USERNAME = PropertiesFileUtil.getProperty("USERNAME");
    private static final String PASSWORD = PropertiesFileUtil.getProperty("PASSWORD");
    private static final String DRIVER = PropertiesFileUtil.getProperty("DRIVER");
    private static final String OUTPUT_DIRECTORY = PropertiesFileUtil.getProperty("OUTPUT_DIRECTORY");
    private static final String FILE_NAME = PropertiesFileUtil.getProperty("FILE_NAME");

    private static void setUp() {
        SqoopOptions.setConnectString(CONNECTION_STRING);
        SqoopOptions.setUsername(USERNAME);
        SqoopOptions.setPassword(PASSWORD);
        SqoopOptions.setDriverClassName(DRIVER);
    }

    public static void main(String... args) throws IOException{
        setUp();
        generateHQL();
    }

    public static void runSqoop() throws IOException{
        setUp();
        generateHQL();
    }


    private static void generateHQL() throws IOException{
        CustomsSqoopTool tool = new CustomsSqoopTool();
        tool.generateDataVaultHQL(SqoopOptions,OUTPUT_DIRECTORY, FILE_NAME);
    }

}
