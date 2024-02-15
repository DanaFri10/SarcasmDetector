import com.google.gson.Gson;
import com.google.gson.JsonObject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class WorkerThread extends Thread{
    private SqsClient sqs;
    private SentimentAnalysisHandler sentimentAnalysisHandler;
    private NamedEntityRecognitionHandler namedEntityRecognitionHandler;
    private Gson gson;
    private ReceiveMessageRequest receiveRequest;
    private String queueUrl;
    private String outQueueUrl;

    public WorkerThread(SqsClient sqs, SentimentAnalysisHandler sentimentAnalysisHandler, NamedEntityRecognitionHandler namedEntityRecognitionHandler, Gson gson, ReceiveMessageRequest receiveRequest, String queueUrl, String outQueueUrl) {
        this.sqs = sqs;
        this.sentimentAnalysisHandler = sentimentAnalysisHandler;
        this.namedEntityRecognitionHandler = namedEntityRecognitionHandler;
        this.gson = gson;
        this.receiveRequest = receiveRequest;
        this.queueUrl = queueUrl;
        this.outQueueUrl = outQueueUrl;
    }

    @Override
    public void run() {
        while(true){
            try {
                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
                for (Message m : messages) {
                    ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).visibilityTimeout(600).build();
                    sqs.changeMessageVisibility(request);
                    System.out.println(m.body());
                    int delimiterIndex = m.body().indexOf(';');
                    String idQueueName = m.body().substring(0, delimiterIndex);
                    String data = m.body().substring(delimiterIndex + 1);
                    delimiterIndex = data.indexOf(';');
                    JsonObject json = gson.fromJson(data.substring(delimiterIndex + 1), JsonObject.class);
                    String text = json.get("text").getAsString();
                    int rating = json.get("rating").getAsInt();
                    int sentiment = sentimentAnalysisHandler.findSentiment(text);
                    Response response = new Response(data.substring(0, delimiterIndex), json.get("link").getAsString(), sentiment, namedEntityRecognitionHandler.printEntities(text), Math.abs(rating - sentiment) >= 3);
                    String jsonResponse = gson.toJson(response);
                    System.out.println(jsonResponse);
                    SendMessageRequest send_msg_request = SendMessageRequest.builder()
                            .queueUrl(outQueueUrl)
                            .messageBody(idQueueName + ";" + jsonResponse)
                            .build();
                    sqs.sendMessage(send_msg_request);
                    DeleteMessageRequest delete = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build();
                    sqs.deleteMessage(delete);
                }
            }catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
    }
}
