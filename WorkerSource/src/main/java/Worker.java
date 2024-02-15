import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class Worker {
    static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    static final String INPUT_QUEUE = "inputQueue";
    static final String OUTPUT_QUEUE = "outputQueue";
    static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static void main(String[] args){
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(INPUT_QUEUE)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        GetQueueUrlRequest getOutQueueRequest = GetQueueUrlRequest.builder()
                .queueName(OUTPUT_QUEUE)
                .build();
        String outQueueUrl = sqs.getQueueUrl(getOutQueueRequest).queueUrl();
        int cores = Runtime.getRuntime().availableProcessors();
        for(int i = 0; i < cores; i++) {
            WorkerThread t = new WorkerThread(sqs, sentimentAnalysisHandler, namedEntityRecognitionHandler, gson, receiveRequest, queueUrl, outQueueUrl);
            t.run();
        }
    }
}
