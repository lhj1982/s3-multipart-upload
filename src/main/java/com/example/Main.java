package com.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.http.entity.ContentType;

import java.io.*;
import java.util.*;

public class Main {
    private static final long PART_SIZE = 100 * 1024 * 1024;

    public static void main(String[] args) throws IOException {
        String bucketName = "jamestest11111";
        String key = "uploaded-file.txt";
        File file = new File("src/main/resources/sample.txt");

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .withRegion("ap-northeast-2")
            .build();

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType("application/x-tar");
        List<PartETag> partETags = new ArrayList<>();
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key, objectMetadata);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

        try (InputStream inputStream = new FileInputStream(file)) {
            byte[] buffer = new byte[(int) PART_SIZE];
            int bytesRead;
            int partNumber = 1;
            System.out.println("Starting uploading file using multipart upload...");
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                System.out.println("Reading part #" + partNumber);
                UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withUploadId(initResponse.getUploadId())
                    .withPartNumber(partNumber++)
                    .withInputStream(new ByteArrayInputStream(buffer, 0, bytesRead))
                    .withPartSize(bytesRead);

                UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadResult.getPartETag());
            }

            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                bucketName, key, initResponse.getUploadId(), partETags);
            s3Client.completeMultipartUpload(compRequest);
            System.out.println("Upload complete. Splitted into " + partETags.size() + " parts");
        } catch (Exception e) {
            s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, initResponse.getUploadId()));
            e.printStackTrace();
        }
    }
}
