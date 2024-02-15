import com.google.gson.reflect.TypeToken;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.FileReader;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Manager {
    public static String LOCAL_TO_MANAGER_QUEUE_URL;
    public static String MANAGER_TO_WORKERS_QUEUE_URL;
    public static String WORKERS_TO_MANAGER_QUEUE_URL;
    public static String BACKUP_QUEUE_URL;
    public static final String OUTPUT_BUCKET = "sarcasmdetectorassignmentmanageroutputs" + (new Date()).getTime();
    public static boolean terminate;
    public static SqsClient sqsClient;
    public static final int MAXIMUM_INSTANCES = 8;
    private Ec2Client ec2;
    public static Map<String, List<Integer>> jobs;

    public Manager() {
        terminate = false;
        sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
        ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        jobs = new HashMap<>();

        MANAGER_TO_WORKERS_QUEUE_URL = createQueue("inputQueue");
        WORKERS_TO_MANAGER_QUEUE_URL = createQueue("outputQueue");
        LOCAL_TO_MANAGER_QUEUE_URL = createFifoQueue("local_to_manager");

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName("backupQueue.fifo")
                .build();
        BACKUP_QUEUE_URL = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

        createBucketIfNotExists(OUTPUT_BUCKET);
    }

    public void startManager() {
        System.out.println("starting");
        closeAllWorkers(ec2);
        purgeQueue(MANAGER_TO_WORKERS_QUEUE_URL);
        purgeQueue(WORKERS_TO_MANAGER_QUEUE_URL);
        ManagerReadThread t1 = new ManagerReadThread(sqsClient, ec2);
        t1.start();
        ManagerWriteThread t2 = new ManagerWriteThread(ec2);
        t2.start();
        //runWorkers(sendMessages/n);
        //listenToMessageFromWorkers();
    }


    private void addToInputQueue(String review) {
        /*
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(review)
                .build();

        SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);

        sendMessages++;
        System.out.println("Message sent. Message ID: " + sendMessageResponse.messageId());*/
    }

    public void createBucketIfNotExists(String bucketName) {
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder().bucket(bucketName).build());
        }
        catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }



    public String createQueue(String queueName) {
        //SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        CreateQueueResponse createQueueResponse = sqsClient.createQueue(createQueueRequest);
        return createQueueResponse.queueUrl();
    }


    public String createFifoQueue(String queueName) {
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName + ".fifo")
                .attributes(attributes) // Set FIFO_QUEUE attribute to true
                .build();
        CreateQueueResponse createQueueResponse = sqsClient.createQueue(createQueueRequest);
        return createQueueResponse.queueUrl();
    }

    public static void closeAllWorkers(Ec2Client ec2){
        List<String> ids = new ArrayList<>();
        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(
                DescribeInstancesRequest.builder()
                        .filters(f -> f.name("instance-state-name").values("running"), g -> g.name("tag:Role").values("Worker").build())
                        .build()
        );

        for (Reservation reservation : describeInstancesResponse.reservations()) {
            for (Instance instance : reservation.instances()) {
                ids.add(instance.instanceId());
            }
        }
        if(!ids.isEmpty()) {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(ids)
                    .build();
            ec2.terminateInstances(request);
            DescribeInstancesRequest instanceRequest = DescribeInstancesRequest.builder()
                    .instanceIds(ids)
                    .build();
            Ec2Waiter ec2Waiter = Ec2Waiter.builder()
                    .overrideConfiguration(b -> b.maxAttempts(100))
                    .client(ec2)
                    .build();
            WaiterResponse<DescribeInstancesResponse> waiterResponse = ec2Waiter.waitUntilInstanceTerminated(instanceRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);
        }
    }

    public static void purgeQueue(String queueUrl){
        PurgeQueueRequest purgeQueueRequest = PurgeQueueRequest.builder().queueUrl(queueUrl).build();
        sqsClient.purgeQueue(purgeQueueRequest);
    }
}



