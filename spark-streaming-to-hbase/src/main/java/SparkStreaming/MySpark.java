package SparkStreaming;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class MySpark
{
    static int counter=1;
    private JavaSparkContext sc;
    private HBaseDBManager db;
    private HashMap<String,String> mapKeywords;
    public MySpark() throws IOException
    {
        this.sc=new JavaSparkContext("local[*]","MySpark",new SparkConf());
        this.db=new HBaseDBManager();
        this.mapKeywords=this.db.GetKeywords();
    }

    public void Process(String key, List<String> l) throws IOException
    {
        JavaRDD<String> list = this.sc.parallelize(l).flatMap(line -> Arrays.asList(line.toLowerCase().split("rt"))).filter(line ->!line.isEmpty());
        JavaPairRDD<String, RedditComments>  redditCommentResults=list.mapToPair(new MyPairFunction(key,this.mapKeywords)).sortByKey();
        this.db.WriteRedditCommentAnalysis(key,redditCommentResults.values());
    }

    static class MyPairFunction implements PairFunction<String, String, RedditComments>,Serializable
    {
        private String key;
        private HashMap<String,String> map;
        public MyPairFunction(String k,HashMap<String,String> m)
        {
            this.key=k;
            this.map=m;
        }

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, RedditComments> call(String _line) throws Exception
        {
            System.out.println(_line);
            String line=_line.toLowerCase();
            System.out.println(line);
            RedditComments redditComment = RedditComments.fromJson(line);

//            if(line.contains("@") && line.contains(":"))
//            {
//                int pos1=line.indexOf("@");
//                int pos2=line.indexOf(":");
//                redditComment.user=line.substring(pos1, pos2);
//            }

            for(String x:this.map.keySet())
            {
                for(String s:this.map.get(x).split(","))
                {
                    if(line.contains(s.toLowerCase()))
                    {
                        System.out.println(x+s);
                        redditComment.keyword.add(new Tuple2<String,String>(x,s));
                    }
                }
            }
            if(redditComment.keyword.isEmpty())
            {
                redditComment.keyword.add(new Tuple2<String,String>("General","general"));
            }
            return new Tuple2<String, RedditComments>(this.key,redditComment);
        }

    }
}
