import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ManagerReadThread extends Thread{
    private SqsClient sqsClient;
    private Ec2Client ec2;
    private boolean backupQueueEmpty;
    private final String AMI_ID = "ami-0b40785c92351a229";

    public ManagerReadThread(SqsClient sqsClient, Ec2Client ec2) {
        this.sqsClient = sqsClient;
        this.ec2 = ec2;
        this.backupQueueEmpty = false;
    }

    @Override
    public void run() {
        while(!backupQueueEmpty) {
            try {
                readFromQueue(true);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while(!Manager.terminate) {
            try {
                readFromQueue(false);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void readFromQueue(boolean backup) throws IOException, InterruptedException {
        String queueUrl = backup ? Manager.BACKUP_QUEUE_URL : Manager.LOCAL_TO_MANAGER_QUEUE_URL;
        ReceiveMessageRequest msgRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(20)
                .visibilityTimeout(6000)
                .build();
        ReceiveMessageResponse msgResponse = sqsClient.receiveMessage(msgRequest);

        List<Message> messages = msgResponse.messages();
        for (Message message : messages) {
            if(message.body().equals("TERMINATE")){
                Manager.terminate = true;
                DeleteMessageRequest deleteMsgRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMsgRequest);
                continue;
            }
            if(backup && message.body().equals("EndOfQueue")) {
                backupQueueEmpty = true;
                DeleteMessageRequest deleteMsgRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMsgRequest);
                continue;
            }
            String msg = message.body();

            if(!backup) {
                pushToBackupQueue(msg);
            }

            String receiptHandle = message.receiptHandle();
            String[] msgParts = msg.split("~");
            String managerToLocalQueue = msgParts[0];
            int n = Math.max(1, Integer.parseInt(msgParts[1]));
            int numOfInputFiles = Integer.parseInt(msgParts[2]);

            System.out.println("Queue name: " + managerToLocalQueue);
            System.out.println("n: " + n);
            System.out.println("Num of files: " + numOfInputFiles);

            String[] bucketArray = new String[numOfInputFiles];
            String[] keyArray = new String[numOfInputFiles];
            for(int i = 3; i < 2*numOfInputFiles + 3; i+=2) {
                bucketArray[(i-3)/2] = msgParts[i];
                keyArray[(i-3)/2] = msgParts[i+1];
                System.out.println("Bucket: " + msgParts[i]);
                System.out.println("Key: " + msgParts[i+1]);
            }
            if(queueExists(managerToLocalQueue)) {
                processFile(bucketArray, keyArray, n, managerToLocalQueue, -1);
            }

            DeleteMessageRequest deleteMsgRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();
            sqsClient.deleteMessage(deleteMsgRequest);
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void pushToBackupQueue(String msg) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(Manager.BACKUP_QUEUE_URL)
                .messageBody(msg)
                .messageDeduplicationId("message"+(new Date()).getTime())
                .messageGroupId("request")
                .build();
        sqsClient.sendMessage(send_msg_request);
    }

    private void processFile(String[] buckets, String[] keys, int n, String queueName, int offset) throws InterruptedException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(30); // Adjust the thread pool size as needed
        int sendMessages = 0;
        Manager.jobs.put(queueName, new LinkedList<>());
        for(int i = 0; i < buckets.length; i++){
            File file = getFilesFromS3(buckets[i],keys[i]);
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                Gson gson = new Gson();

                while ((line = reader.readLine()) != null) {
                    JsonObject jsonObject = gson.fromJson(line, JsonObject.class);
                    JsonArray reviews = jsonObject.getAsJsonArray("reviews");

                    if (reviews != null) {
                        for (JsonElement reviewElement : reviews) {
                            JsonObject reviewObject = reviewElement.getAsJsonObject();
                            //addToInputQueue(reviewObject.toString());
                            //File file = new File(filePath);
                            if(sendMessages > offset) {
                                String reviewString = sendMessages + "$" + queueName + ";" + keys[i] + ";" + reviewObject.toString();
                                Manager.jobs.get(queueName).add(sendMessages);
                                executorService.submit(new InsertToQueueTask(reviewString, Manager.MANAGER_TO_WORKERS_QUEUE_URL));
                            }
                            sendMessages++;
                        }
                    } else {
                        System.err.println("No reviews found in the JSON file.");
                    }
                }
            }
            catch (Exception e) {
                System.err.println("Error processing the downloaded JSON file: " + e.getMessage());
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        int numWorkers = sendMessages / n;
        if(numWorkers == 0) {
            numWorkers = 1;
        }
        runWorkers(numWorkers);

    }

    public void runWorkers(int num) {
        int activeWorkers = countActiveWorkers();
        int newWorkers = (num > Manager.MAXIMUM_INSTANCES) ? Manager.MAXIMUM_INSTANCES : num - activeWorkers;

        if(newWorkers > 0) {
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_LARGE)
                    .imageId(AMI_ID)
                    .maxCount(newWorkers)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(
                            createUserData().getBytes()))
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("EMR_EC2_DefaultRole").build())
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            List<Instance> instances = response.instances();
            List<String> instanceIds = new ArrayList<>();
            for(Instance instance : instances) {
                instanceIds.add(instance.instanceId());
            }

            Tag tag = Tag.builder()
                    .key("Role")
                    .value("Worker")
                    .build();


            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceIds)
                    .tags(tag)
                    .build();
            ec2.createTags(tagRequest);
        }
    }

    public File getFilesFromS3(String bucketName, String key) throws IOException {
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        File file = null;
        file = File.createTempFile(key, null);
        ResponseInputStream<GetObjectResponse> s3Object = s3.getObject(getObjectRequest);
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        s3Object.transferTo(fileOutputStream);
        fileOutputStream.close();

        return file;
    }

    public String createUserData() {
        String ret = "#!/bin/bash\n";
        ret += "java -Xmx77500M -jar Worker.jar\n";
        return ret;
    }

    private int countActiveWorkers() {
        Filter filterByRunning = Filter.builder().name("instance-state-name").values("running","pending").build();
        Filter filterByWorker = Filter.builder().name("tag:Role").values("Worker").build();
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().filters(filterByRunning, filterByWorker).build();
        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
        int size = 0;
        for(Reservation reservation : describeInstancesResponse.reservations()){
            size += reservation.instances().size();
        }
        return size;
    }

    private boolean queueExists(String queueName) {
        try {
            GetQueueUrlResponse response = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        }
    }

}
