import java.util.List;

public class Response {
    private String fromFile;
    private String link;
    private int sentiment;
    private List<Pair> nes;
    private boolean sarcasm;

    public Response(String fromFile, String link, int sentiment, List<Pair> nes, boolean sarcasm) {
        this.fromFile = fromFile;
        this.link = link;
        this.sentiment = sentiment;
        this.nes = nes;
        this.sarcasm = sarcasm;
    }

    public String getFromFile() {
        return getFromFile();
    }

    public String getLink() {
        return link;
    }

    public int getSentiment() {
        return sentiment;
    }

    public List<Pair> getNes() {
        return nes;
    }

    public boolean isSarcasm() {
        return sarcasm;
    }
}
