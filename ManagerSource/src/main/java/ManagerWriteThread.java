import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ManagerWriteThread extends Thread{
    private Map<String, FileWriter> writerMap;
    private Map<String, File> fileMap;
    private Ec2Client ec2;

    public ManagerWriteThread(Ec2Client ec2) {
        writerMap = new HashMap<>();
        fileMap = new HashMap<>();
        this.ec2 = ec2;
    }

    @Override
    public void run() {
        try {
            listenToMessageFromWorkers();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listenToMessageFromWorkers() throws IOException {
        /*resultFile.createNewFile();

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(WORKERS_TO_MANAGER_QUEUE).build();
        String queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();*/
        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();

        while(!(Manager.terminate && Manager.jobs.isEmpty())) {
            ReceiveMessageRequest msgRequest = ReceiveMessageRequest.builder()
                    .queueUrl(Manager.WORKERS_TO_MANAGER_QUEUE_URL)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(1)
                    .build();
            ReceiveMessageResponse msgResponse = sqsClient.receiveMessage(msgRequest);

            List<Message> messages = msgResponse.messages();

            for (Message message : messages) {
                System.out.println(message.body());
                //String msg = message.body() + "\n";
                /*synchronized (lock) {
                    fileWriter.write(msg);
                    receivedMessages++;
                }

                DeleteMessageRequest deleteMsgRequest = DeleteMessageRequest.builder()
                        .queueUrl(WORKERS_TO_MANAGER_QUEUE_URL)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMsgRequest);*/
                int delimiterIndex = message.body().indexOf(';');
                String idQueueName = message.body().substring(0, delimiterIndex);
                int dollarSign = idQueueName.indexOf('$');
                int id = Integer.parseInt(idQueueName.substring(0, dollarSign));
                String queueName = idQueueName.substring(dollarSign + 1);
                if(Manager.jobs.containsKey(queueName) && Manager.jobs.get(queueName).contains(id)) {
                    getWriter(queueName).write(message.body().substring(delimiterIndex + 1) + "\n");
                    DeleteMessageRequest deleteMsgRequest = DeleteMessageRequest.builder()
                            .queueUrl(Manager.WORKERS_TO_MANAGER_QUEUE_URL)
                            .receiptHandle(message.receiptHandle())
                            .build();
                    Manager.sqsClient.deleteMessage(deleteMsgRequest);
                    Manager.jobs.get(queueName).remove(Integer.valueOf(id));
                }
                if(Manager.jobs.get(queueName).isEmpty()){
                    Manager.jobs.remove(queueName);
                    writerMap.get(queueName).close();
                    writerMap.remove(queueName);
                    returnFile(queueName);
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        terminate();
    }

    private FileWriter getWriter(String queueName) throws IOException {
        if(!writerMap.containsKey(queueName)){
            File resultFile = new File(queueName + "_result.txt");
            resultFile.createNewFile();
            fileMap.put(queueName, resultFile);
            writerMap.put(queueName, new FileWriter(resultFile, true));
        }
        return writerMap.get(queueName);
    }

    public void returnFile(String queueName) {
        uploadFileToS3(fileMap.get(queueName));
        fileMap.get(queueName).delete();
        fileMap.remove(queueName);
        String messageBody = Manager.OUTPUT_BUCKET + "~" + queueName + "_result.txt";

        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        String queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);

        System.out.println("Message sent. Message ID: " + sendMessageResponse.messageId());
    }

    public void uploadFileToS3(File file) {
        //InputStream fileInputStream = new FileInputStream(file);
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(Manager.OUTPUT_BUCKET)
                .key(file.getName().substring(0, file.getName().indexOf(".txt")+4)) // Use the file name as the S3 object key
                .build();

        s3.putObject(putObjectRequest, file.toPath());
    }



    private void terminate(){
        System.out.println("All done, terminating...");
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
        System.out.println("Terminated.");
        //remove bucket
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(Manager.OUTPUT_BUCKET).build();
        ListObjectsResponse objectListing = s3.listObjects(listObjectsRequest);
        while (true) {
            Iterator<S3Object> objIter = objectListing.contents().iterator();
            while (objIter.hasNext()) {
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(Manager.OUTPUT_BUCKET).key(objIter.next().key()).build();
                s3.deleteObject(deleteObjectRequest);
            }

            // If the bucket contains many objects, the listObjects() call
            // might not return all of the objects in the first listing. Check to
            // see whether the listing was truncated. If so, retrieve the next page of
            // objects
            // and delete them.
            if (objectListing.isTruncated()) {
                objectListing = s3.listObjects(listObjectsRequest);
            } else {
                break;
            }
        }
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(Manager.OUTPUT_BUCKET).build();
        s3.deleteBucket(deleteBucketRequest);
        //purge backup
        Manager.purgeQueue(Manager.BACKUP_QUEUE_URL);
        //delete queues
        String[] queuesToDelete = {Manager.WORKERS_TO_MANAGER_QUEUE_URL, Manager.MANAGER_TO_WORKERS_QUEUE_URL, Manager.LOCAL_TO_MANAGER_QUEUE_URL};
        for(String queueToDelete : queuesToDelete){
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueToDelete).build();
            Manager.sqsClient.deleteQueue(deleteQueueRequest);
        }
        //terminate the manager
        ids = new LinkedList<>();
        describeInstancesResponse = ec2.describeInstances(
                DescribeInstancesRequest.builder()
                        .filters(f -> f.name("instance-state-name").values("running"))
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
        }
    }
}
