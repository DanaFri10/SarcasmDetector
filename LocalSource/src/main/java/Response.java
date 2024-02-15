import java.util.List;
import com.google.gson.JsonObject;

public class Response {
    private String fromFile;
    private String link;
    private int sentiment;
    private List<JsonObject> nes;
    private boolean sarcasm;

    public Response(String fromFile, String link, int sentiment, List<JsonObject> nes, boolean sarcasm) {
        this.fromFile = fromFile;
        this.link = link;
        this.sentiment = sentiment;
        this.nes = nes;
        this.sarcasm = sarcasm;
    }

    public String getFromFile(){ return fromFile; }

    public String getLink() {
        return link;
    }

    public int getSentiment() {
        return sentiment;
    }

    public List<JsonObject> getNes() {
        return nes;
    }

    public boolean isSarcasm() {
        return sarcasm;
    }
}
