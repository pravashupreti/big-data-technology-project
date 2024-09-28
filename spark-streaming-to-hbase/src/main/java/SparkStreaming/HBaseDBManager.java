package SparkStreaming;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

public class HBaseDBManager
{
    public static final Map<String, String> map = new HashMap<>();
    static {
        map.put("trading", "Trading");
        map.put("finance", "Finance");
        map.put("invest", "Investing Investment");
        map.put("career", "Career");
        map.put("options", "Options");
        map.put("market", "Market");
        map.put("portfolio", "Portfolio");
        map.put("strategy", "Strategy");
        map.put("stock", "Stock");
        map.put("dividend", "Dividend");
        map.put("advice", "Advice");
        map.put("job", "Job");
        map.put("watchlist", "Watchlist");
        map.put("earnings", "Earnings");
        map.put("forex", "Forex");
        map.put("analysis", "Analysis");
        map.put("daytrading", "Day Trading");
        map.put("spy", "Spy");
    }
    private Configuration hbaseConfig;
    private int rowkeyAnalysis=0;
    private final String TABLE_NAME="table_reddit_comment_analysis";
    int count=0;
    public HBaseDBManager() throws IOException
    {
        this.hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "54.226.131.75"); // Set Zookeeper server IP which I have setup on AWS
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181"); // Default Zookeeper client port
        hbaseConfig.set("hbase.rpc.timeout", "60000");
        hbaseConfig.set("hbase.client.retries.number", "3");

        this.DefaultValues();
        this.rowkeyAnalysis=this.GetMaxRownum();
    }

    private void DefaultValues() throws IOException
    {
        try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig);
             Admin admin = connection.getAdmin())
        {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("table_keywords"));
            table.addFamily(new HColumnDescriptor("type_family").setCompressionType(Algorithm.NONE));
            table.addFamily(new HColumnDescriptor("keywords_family"));

            if (admin.tableExists(table.getTableName()))
            {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            if (!admin.tableExists(table.getTableName()))
            {
                admin.createTable(table);
                Table tbl = connection.getTable(TableName.valueOf("table_keywords"));

//                Put put1 = new Put(Bytes.toBytes("1"));
//                put1.addColumn(Bytes.toBytes("type_family"),Bytes.toBytes("type"),Bytes.toBytes("Genera Redit Questions"));
//                put1.addColumn(Bytes.toBytes("keywords_family"),Bytes.toBytes("keywords"),Bytes.toBytes("askreddit"));
//                tbl.put(put1);

                int rowIndex = 1;
                for (Map.Entry<String, String> entry : map.entrySet()) {

                    String value = entry.getValue(); // Use the map's value as the column value
                    Put put = new Put(Bytes.toBytes(rowIndex));

                    // Add columns (you can customize column family/qualifiers as needed)
                    put.addColumn(Bytes.toBytes("type_family"), Bytes.toBytes("type"), Bytes.toBytes(value));
                    put.addColumn(Bytes.toBytes("keywords_family"), Bytes.toBytes("keywords"), Bytes.toBytes(value.toLowerCase()));

                    // Insert the Put object into the HBase table
                    tbl.put(put);
                    rowIndex++;

                }


                tbl.close();
            }
        }
    }

    public HashMap<String,String> GetKeywords() throws IOException
    {
        HashMap<String,String> map=new HashMap<String,String>();

        try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig))
        {
            Table tbl = connection.getTable(TableName.valueOf("table_keywords"));
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(10000);
            scan.setMaxVersions(10);
            ResultScanner scanner = tbl.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next())
            {
                String type="";
                String keywords="";
                for (Cell cell : result.rawCells())
                {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if(family.equalsIgnoreCase("type_family") && column.equalsIgnoreCase("type"))
                    {
                        type=Bytes.toString(CellUtil.cloneValue(cell));
                    }
                    else if(family.equalsIgnoreCase("keywords_family") && column.equalsIgnoreCase("keywords"))
                    {
                        keywords=Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }

                if(!map.containsKey(type))
                {
                    map.put(type, keywords);
                }
                else
                {
                    map.replace(type, map.get(type)+","+keywords);
                }
            }
        }

        return map;
    }

    public void WriteRedditCommentAnalysis(String key,JavaRDD<RedditComments> rdd) throws IOException
    {
        try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig);
             Admin admin = connection.getAdmin())
        {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor("key_family").setCompressionType(Algorithm.NONE));
            table.addFamily(new HColumnDescriptor("reddit_comment_family"));
            if (!admin.tableExists(table.getTableName()))
            {
                admin.createTable(table);
            }

            Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
//			int count=0;
            for(RedditComments rd:rdd.collect())
            {
                System.out.println(rd);
                Put put = new Put(Bytes.toBytes(String.valueOf(++this.rowkeyAnalysis)));
                put.addColumn(Bytes.toBytes("key_family"),Bytes.toBytes("key"),Bytes.toBytes(key));
                put.addColumn(Bytes.toBytes("key_family"),Bytes.toBytes("user"),Bytes.toBytes(rd.author));
                put.addColumn(Bytes.toBytes("key_family"),Bytes.toBytes("timestamp"),Bytes.toBytes( String.valueOf(rd.timestamp)));
                put.addColumn(Bytes.toBytes("reddit_comment_family"),Bytes.toBytes("reddit_comment_analysis"),Bytes.toBytes(rd.GetStatement()));
                put.addColumn(Bytes.toBytes("reddit_comment_family"),Bytes.toBytes("keyword"),Bytes.toBytes(rd.GetFoundKeywords()));
                tbl.put(put);
                count++;
            }
            tbl.close();

            System.out.println("table_reddit_comment_analysis written rows count:" + count);
        }
    }

    @SuppressWarnings({ "finally", "deprecation" })
    private int GetMaxRownum()
    {
        try
        {
            @SuppressWarnings("resource")
            Result result=new HTable(this.hbaseConfig,TABLE_NAME).getRowOrBefore(Bytes.toBytes("9999"),Bytes.toBytes(""));
            return Integer.parseInt(Bytes.toString(result.getRow()));
        }
        catch(Exception ex){}
        finally {return 0;}
    }


}
