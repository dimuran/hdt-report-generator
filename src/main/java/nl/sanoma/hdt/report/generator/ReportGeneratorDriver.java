/*
 * Copyright (C) 2014 Richárd Ernő Kiss
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package nl.sanoma.hdt.report.generator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Richárd Ernő Kiss
 */
public class ReportGeneratorDriver extends Configured implements Tool {

    public static final String SEPARATOR = "þ";
    private static final String DB_CONFIG_FILE_NAME = "dbconfig.properties";
    private static final String DB_CONFIG_DRIVER = "driver";
    private static final String DB_CONFIG_URL = "url";
    private static final String DB_CONFIG_USER = "user";
    private static final String DB_CONFIG_PASSWORD = "password";
    private static final String DB_CONFIG_TABLE_NAME = "tableName";

    /**
     *
     * @param args array of string arguments
     * @return returns the exitCode of the job
     * @throws Exception if an error occurs
     */
    @Override
    public int run(String[] args) throws Exception {
        if(args.length<3){
         System.err.println("Usage: hadoop jar hdt-report-generator.jar DB_EXPORT_PATH INPUT_DIRECTORY OUTPUT_DIRECTORY -libjars LIBJARS\n"
                 + "DB_EXPORT_PATH - the directory in HDFS where the DB file will be exported in MapFile format\n"
                 + "INPUT_DIRECTORY - the directory in HDFS where the log files are\n"
                 + "OUTPUT_DIRECTORY - the directory in HDFS where the report will be placed\n"
                 + "LIBJARS - path to the DB connector dependency (example: mysql-connector-java.jar)");
        }else{
        if (importDBToMapFile(args[0])) {
            return generateReport(args[0], args[1], args[2]) ? 0 : 1;
        }
        }
            return 1;
    }

    /**
     * Application entry point.
     *
     * @param args array of string arguments
     * @throws Exception if an error occurs
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReportGeneratorDriver(), args);
        System.exit(exitCode);
    }

    /**
     * Job to import the values from DB to a MapFile in HDFS
     *
     * @param dBOutputPath the path where the MapFile output will be created
     * @return returns the exitCode of the job
     * @throws ConfigurationException
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public Boolean importDBToMapFile(String dBOutputPath) throws ConfigurationException, IOException, InterruptedException, ClassNotFoundException {

        PropertiesConfiguration dBConfig = new PropertiesConfiguration(DB_CONFIG_FILE_NAME);
        //TODO: error handling if config file is missing

        //create MapFile from DB job
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        job.setJobName("DB to MapFile import");
        job.setJarByClass(ReportGeneratorDriver.class);
        job.setMapperClass(DBToMapFileMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        MapFileOutputFormat.setOutputPath(job, new Path(dBOutputPath));
        //TODO: add compression to MapFile
        job.setNumReduceTasks(0);
        DBConfiguration.configureDB(conf, dBConfig.getString(DB_CONFIG_DRIVER), dBConfig.getString(DB_CONFIG_URL), dBConfig.getString(DB_CONFIG_USER), dBConfig.getString(DB_CONFIG_PASSWORD));
        String[] fields = {"id", "name", "category", "price"};
        DBInputFormat.setInput(job, DBInputWritable.class, dBConfig.getString(DB_CONFIG_TABLE_NAME), null, "id", fields);

        return job.waitForCompletion(true);
    }

    /**
     * Job to join the data and the metadata from distributed cache and
     * calculate the revenue by quarter and most popular product category for user
     *
     * @param dBPath the path of the import MapFile
     * @param inputPath the path of the logs directory
     * @param outputPath the path of the output directory
     * @return returns the exitCode of the job
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public Boolean generateReport(String dBPath, String inputPath, String outputPath) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();

        job.setJobName("Repor Generator");
        DistributedCache.addCacheFile(new URI(dBPath), conf);
        job.setJarByClass(ReportGeneratorDriver.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setPartitionerClass(KeyDataPartitioner.class);
        job.setGroupingComparatorClass(KeyDataGroupingComparator.class);
        job.setSortComparatorClass(KeyDataComparator.class);
        job.setMapperClass(ReportGeneratorMapper.class);
        job.setMapOutputKeyClass(KeyData.class);
        job.setMapOutputValueClass(ValueData.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(ReportGeneratorReducer.class);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true);
    }
}
