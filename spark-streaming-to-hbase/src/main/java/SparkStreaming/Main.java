package SparkStreaming;

import java.util.ArrayList;
import java.util.List;

public class Main {

    static String lastKey="";
    static List<String> list=new ArrayList<String>();
    public static void main(String[] args) throws Exception
    {
        MySpark spark=new MySpark();
        MyResources resources=new MyResources();
        MyKafkaConsumer consumer=new MyKafkaConsumer(resources);
        consumer.Wait(()-> {
                    try {
                        Thread.sleep(3000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return true;
                }
                , (String key, String val,boolean new_key)->
                {
                    if(lastKey.isEmpty())
                        lastKey=key;
                    if(new_key)
                    {
                        try
                        {
                            if(!list.isEmpty())
                                spark.Process(lastKey, list);
                        }
                        catch (Exception e) { e.printStackTrace();}

                        lastKey=key;
                        list.clear();
                    }

                    list.add(val);
                });
    }
}