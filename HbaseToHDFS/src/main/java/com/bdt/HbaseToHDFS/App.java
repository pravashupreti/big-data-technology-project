package com.bdt.HbaseToHDFS;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.AnalysisException;

public class App {

    public static void main(String[] args) throws AnalysisException, IOException {

        SparkConf conf= new SparkConf().setAppName("SparkSQL").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL2")
                .config(conf)
                .getOrCreate();

        showRedditCommentAnalysis(sc,spark);
        spark.stop();
        sc.close();
    }

    private static void showRedditCommentAnalysis(JavaSparkContext sc,SparkSession spark) throws IOException {

        JavaRDD<RedditComment> redditCommentsRDD=sc.parallelize(new HBaseReader().GetRedditCommentAnalysis());

        String schemaString = "key user reddit_comment_analysis keyword timestamp";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" "))
        {
           StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);

            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = redditCommentsRDD.map((Function<RedditComment, Row>) record ->
        {
            System.out.println(record);
            if (record.key !=null) {
                return RowFactory.create(record.key , record.user,record.GetStatement().isEmpty()?"General":record.GetStatement(),record.GetFoundKeywords(),record.timestamp);
            }
            return null;
        });

        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        dataFrame.createOrReplaceTempView("reddit_comment");

        Dataset<Row> redditCommentResult = spark.sql("SELECT * FROM ( SELECT key, user, reddit_comment_analysis, explode(split(keyword, ',')) AS keyword, timestamp FROM reddit_comment WHERE key IS NOT NULL ) AS exploded_keywords");
        redditCommentResult.show(50);

        Dataset<Row> redditCommentCount = spark.sql("SELECT keyword, COUNT(*) AS count FROM ( SELECT explode(split(reddit_comment_analysis, ',')) AS keyword FROM reddit_comment ) AS exploded_keywords GROUP BY keyword ORDER BY count DESC");
        redditCommentCount.show(50);

//        redditCommentResult.write().mode("overwrite").option("header","false").option("delimiter", ";").csv("hdfs://users/cloudera/CommentResultTable");
//        redditCommentCount.write().mode("overwrite").option("header","false").option("delimiter", ";").csv("hdfs://users/cloudera/CommentCountTable");
        redditCommentResult.write().mode("overwrite").option("header","false").option("delimiter", ";").csv("file:///Users/pravash/workspace/hdfs/CommentResultTable");
        redditCommentCount.write().mode("overwrite").option("header","false").option("delimiter", ";").csv("file:///Users/pravash/workspace/hdfs/CommentCountTable");

    }
}

