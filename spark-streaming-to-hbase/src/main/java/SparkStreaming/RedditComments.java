package SparkStreaming;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RedditComments implements Serializable {
    private static final long serialVersionUID = 154265L;

    // New attributes to store the JSON fields
    public String id;
    public String name;
    public String author;
    public String body;
    public String subreddit;
    public int upvotes;
    public int downvotes;
    public boolean over_18;
    public double timestamp;
    public String permalink;

    public List<Tuple2<String, String>> keyword;

    public RedditComments() {
        this.keyword = new ArrayList<>();
    }

    public String GetStatement() {
        if (this.keyword.isEmpty())
            return "";
        return this.keyword.stream().map(t -> t._1()).collect(Collectors.joining(","));
    }

    public String GetFoundKeywords() {
        if (this.keyword.isEmpty())
            return "";
        return this.keyword.stream().map(t -> t._2()).collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return this.author + "|" + this.GetStatement() + "|" + this.GetFoundKeywords();
    }

    // Method to deserialize JSON string to RedditComments object
    public static RedditComments fromJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        RedditComments comment = null;
        try {
            comment = mapper.readValue(jsonString, RedditComments.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return comment;
    }
}
