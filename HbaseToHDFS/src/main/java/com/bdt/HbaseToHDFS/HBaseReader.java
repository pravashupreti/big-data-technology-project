package com.bdt.HbaseToHDFS;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseReader
{
    private Configuration hbaseConfig;
    private final String TABLE_NAME="table_reddit_comment_analysis";
    public HBaseReader()
    {
        this.hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "54.226.131.75");
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181"); 
        hbaseConfig.set("hbase.zookeeper.property.tickTime", "3000"); 
        hbaseConfig.set("hbase.rpc.timeout", "60000");
        hbaseConfig.set("hbase.client.retries.number", "3");

    }

    public List<RedditComment> GetRedditCommentAnalysis() throws IOException
    {
        List<RedditComment> redditCommentList=new ArrayList<RedditComment>();

        try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig))
        {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(10000);
            scan.setMaxVersions(10);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next())
            {
                RedditComment redditComment=new RedditComment();
                for (Cell cell : result.rawCells())
                {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if(family.equalsIgnoreCase("key_family"))
                    {
                        if(column.equalsIgnoreCase("key"))
                            redditComment.key=Bytes.toString(CellUtil.cloneValue(cell));
                        else if(column.equalsIgnoreCase("user"))
                            redditComment.user=Bytes.toString(CellUtil.cloneValue(cell));
                        else if(column.equalsIgnoreCase("timestamp")) {
                            String timestampLong = Bytes.toString(CellUtil.cloneValue(cell));
                            redditComment.timestamp = timestampLong;
                        }
                    }
                    else if(family.equalsIgnoreCase("reddit_comment_family"))
                    {
                        if(column.equalsIgnoreCase("reddit_comment_analysis"))
                        {
                            redditComment.PutStatements(Bytes.toString(CellUtil.cloneValue(cell)));
                        }
                        else if(column.equalsIgnoreCase("keyword")) {
                            redditComment.PutKeywords(Bytes.toString(CellUtil.cloneValue(cell)));
                        }
                    }
                }

                if(!redditComment.key.isEmpty() && !redditComment.user.isEmpty())
                    redditCommentList.add(redditComment);
            }
        }

        return redditCommentList;
    }
}
