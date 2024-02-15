import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class InsertToQueueTask implements Runnable{
    private String review;
    private String queueUrl;
    private static final Object lock = new Object();

    public InsertToQueueTask(String review, String queueUrl) {
        this.review = review;
        this.queueUrl = queueUrl;
    }

    public void run() {
        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(review)
                .build();

        SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);

        System.out.println("Message sent. Message ID: " + sendMessageResponse.messageId());
    }

}
