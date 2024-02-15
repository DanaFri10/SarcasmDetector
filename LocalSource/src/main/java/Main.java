import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    static final String LINUX = "ami-0b400563b3304e69c";
    static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    static final String TERMINATE = "terminate";
    static final String QUEUE_OUT_NAME = "local_to_manager.fifo";
    static String BACKUP_QUEUE_URL;
    static boolean received = false;
    static SqsClient sqs;
    static S3Client s3;
    static Ec2Client ec2;

    public static void main(String[] args){
        if(!checkArgs(args)) {
            System.out.println("Invalid arguments.");
        }
        else {
            try {
                startLocal(args);
            }
            catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void startLocal(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        s3 = S3Client.builder().region(Region.US_EAST_1).build();
        ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        ManagerReviver reviver = new ManagerReviver();
        boolean terminate = args[args.length - 1].equals(TERMINATE);
        int amountOfFiles = (args.length - (terminate ? 2 : 1)) / 2;
        String[] inFiles = new String[amountOfFiles];
        String[] outFiles = new String[amountOfFiles];
        for (int i = 0; i < amountOfFiles; i++) {
            inFiles[i] = args[i];
            outFiles[i] = args[amountOfFiles + i];
        }
        String queueInName = "queueIn" + (new Date()).getTime();
        try {
            CreateQueueRequest request2 = CreateQueueRequest.builder()
                    .queueName(queueInName)
                    .build();
            sqs.createQueue(request2);
            createFifoQueue(QUEUE_OUT_NAME, sqs);
            BACKUP_QUEUE_URL = createFifoQueue("backupQueue.fifo", sqs);
            System.out.println("Created queues.");
        } catch (QueueNameExistsException e) {
            throw e;
        }
        try {
            startManager();
            System.out.println("Started manager.");
        }
        catch(Exception e) {
            throw new Exception("Failed to start manager.");
        }
        reviver.start();
        String bucketName = "localbucket" + (new Date()).getTime();
        String n = args[amountOfFiles * 2];
        String request = queueInName + "~" + n + "~" + amountOfFiles;
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            for (String fileName : inFiles) {
                s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(fileName).build(), RequestBody.fromFile(new File(fileName)));
                request += "~" + bucketName + "~" + fileName;
            }
        }
        catch(Exception e) {
            throw new Exception("Failed to upload files to s3.");
        }

        String queueUrl;
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(QUEUE_OUT_NAME)
                    .build();
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(request)
                    .messageDeduplicationId("message" + (new Date()).getTime())
                    .messageGroupId("request")
                    .build();
            sqs.sendMessage(send_msg_request);
            System.out.println("Sent message to manager.");
        }
        catch(Exception e) {
            throw new Exception("Failed to send message to manager.");
        }

        try {
            GetQueueUrlRequest getInQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueInName)
                    .build();
            String queueInUrl = sqs.getQueueUrl(getInQueueRequest).queueUrl();
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueInUrl)
                    .build();

            String locBucket = "";
            String locKey = "";
            while (!received) {
                List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
                if (!messages.isEmpty()) {
                    received = true;
                    System.out.println("Received message from manager.");
                }
                for (Message m : messages) {
                    int delimiterIndex = m.body().indexOf('~');
                    locBucket = m.body().substring(0, delimiterIndex);
                    locKey = m.body().substring(delimiterIndex + 1);
                }
            }

            GetObjectRequest getFile = GetObjectRequest.builder().bucket(locBucket).key(locKey).build();
            ResponseBytes<GetObjectResponse> fileBytes = s3.getObjectAsBytes(getFile);
            String data = fileBytes.asUtf8String();
            String[] jsons = data.split("\n");
            String[] htmlDivsArray = new String[amountOfFiles];
            for (int i = 0; i < amountOfFiles; i++) {
                htmlDivsArray[i] = "";
            }
            for (String json : jsons) {
                Response res = gson.fromJson(json, Response.class);
                htmlDivsArray[getFileIndex(res.getFromFile(), inFiles)] += toHTMLDiv(res);
            }
            for (int i = 0; i < amountOfFiles; i++) {
                File outHtml = new File(outFiles[i]);
                try {
                    outHtml.createNewFile();
                    System.out.println("File created: " + outHtml.getName());
                    String html = "<html>\n";
                    html += style();
                    html += htmlDivsArray[i];
                    html += "</html>";
                    Files.write(Path.of(outHtml.getPath()), html.getBytes(StandardCharsets.UTF_8));
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
            if (terminate) {
                SendMessageRequest send_terminate_request = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(TERMINATE.toUpperCase())
                        .messageDeduplicationId("message" + (new Date()).getTime())
                        .messageGroupId("request")
                        .build();
                sqs.sendMessage(send_terminate_request);
                System.out.println("Sent termination message.");
            }
            reviver.STOP = true;
            clean(bucketName, queueInName, sqs, s3);
            long runTime = System.currentTimeMillis() - start;
            long seconds = runTime/1000;
            System.out.println("Run time: " + seconds/60 + " minutes, " + seconds%60 + " seconds.");
        }
        catch (Exception e) {
            throw new Exception("Failed to get output.");
        }
    }

    private static boolean checkArgs(String[] args) {
        if(args.length < 3) {
            return false;
        }
        boolean terminate = args[args.length - 1].equals(TERMINATE);
        String nStr = terminate ? args[args.length - 2] : args[args.length - 1];
        try {
            int n = Integer.parseInt(nStr);
            if(n <= 0) {
                return false;
            }
        }
        catch(Exception e) {
            return false;
        }
        int remainingArgs = terminate ? args.length - 2 : args.length - 1;
        if(remainingArgs % 2 != 0) {
            return false;
        }
        for(int i = 0; i < remainingArgs / 2; i++) {
            File input = new File(args[i]);
            if(!input.exists()) {
                return false;
            }
        }
        return true;
    }

    public static String createUserData(){
        String ret = "#!/bin/bash\n";
        ret += "java -jar Manager.jar\n";
        return ret;
    }

    public static void clean(String bucket, String queue, SqsClient sqs, S3Client s3){
        ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(bucket).build();
        ListObjectsResponse objectListing = s3.listObjects(listObjectsRequest);
        while (true) {
            Iterator<S3Object> objIter = objectListing.contents().iterator();
            while (objIter.hasNext()) {
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(objIter.next().key()).build();
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
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        s3.deleteBucket(deleteBucketRequest);
        //delete queue
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queue).build();
        GetQueueUrlResponse response = sqs.getQueueUrl(getQueueUrlRequest);
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(response.queueUrl()).build();
        sqs.deleteQueue(deleteQueueRequest);
    }

    public static String colorFromSentiment(int sentiment){
        if(sentiment == 1){
            return "#d00000";
        }else if(sentiment == 2){
            return "#ff0000";
        }else if(sentiment == 3){
            return "#000000";
        }else if(sentiment == 4){
            return "#00ff00";
        }
        return "#006600";
    }

    public static String stringFromSentiment(int sentiment){
        if(sentiment == 1){
            return "vnegative";
        }else if(sentiment == 2){
            return "negative";
        }else if(sentiment == 3){
            return "neutral";
        }else if(sentiment == 4){
            return "positive";
        }
        return "vpositive";
    }

    public static String style(){
        String style = "<style>\n";
        for(int i = 1; i <= 5; i++){
            style += "." + stringFromSentiment(i) + "{\n";
            style += "\tcolor: " + colorFromSentiment(i) +";\n";
            style += "}\n\n";
        }
        style += "</style>\n";
        return style;
    }

    public static String toHTMLDiv(Response res){
        String ret = "<div>\n";
        ret += "<a class=\"" + stringFromSentiment(res.getSentiment()) + "\" href=\"" + res.getLink() + "\">" + res.getLink() + "</a>\n";
        ret += "<p>NEs: [";
        boolean remove = false;
        for(JsonObject obj : res.getNes()){
            if(obj.get("tag").getAsString().equals("PERSON") || obj.get("tag").getAsString().equals("ORGANIZATION") || obj.get("tag").getAsString().equals("LOCATION")){
                ret += obj.get("word").getAsString() + ", ";
                remove = true;
            }
        }
        if(remove) {
            ret = ret.substring(0, ret.length() - 2);
        }
        ret += "]</p>\n";
        ret += "<p>Sarcasm: " + (res.isSarcasm() ? "Yes" : "No") + "</p>\n";
        ret += "</div>\n";
        ret += "<hr/>\n";
        return ret;
    }

    public static int getFileIndex(String fromFile, String[] inFiles){
        for(int i = 0; i < inFiles.length; i++){
            if(fromFile.equals(inFiles[i])){
                return i;
            }
        }
        return -1;
    }

    public static String createFifoQueue(String queueName, SqsClient sqs) {
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(attributes) // Set FIFO_QUEUE attribute to true
                .build();
        CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
        return createQueueResponse.queueUrl();
    }

    private static boolean isMansagerUp() {
        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().filters(Filter.builder().name("tag:Name").values("Manager").build(), Filter.builder().name("instance-state-name").values("running","pending").build()).build();
        DescribeInstancesResponse response = ec2.describeInstances(describeInstancesRequest);
        for(Reservation reservation : response.reservations()){
            if(!reservation.instances().isEmpty()){
                return true;
            }
        }
        return false;
    }

    public static void startManager() {
        if(!isMansagerUp() && !received) {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(BACKUP_QUEUE_URL)
                    .messageBody("EndOfQueue")
                    .messageDeduplicationId("message"+(new Date()).getTime())
                    .messageGroupId("request")
                    .build();
            SendMessageResponse sendMessageResponse = sqs.sendMessage(sendMessageRequest);


            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T2_MEDIUM)
                    .imageId(LINUX)
                    .maxCount(1)
                    .minCount(1)
                    .userData(Base64.getEncoder().encodeToString(createUserData().getBytes()))
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                    .build();
            RunInstancesResponse runInstancesResponse = ec2.runInstances(runRequest);
            String instanceId = runInstancesResponse.instances().get(0).instanceId();
            Tag tag = Tag.builder()
                    .key("Name")
                    .value("Manager")
                    .build();
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "Successfully started EC2 instance %s based on AMI %s\n",
                        instanceId, LINUX);

            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }

}
