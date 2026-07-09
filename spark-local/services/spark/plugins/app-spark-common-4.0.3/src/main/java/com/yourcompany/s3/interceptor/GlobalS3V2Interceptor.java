package com.yourcompany.s3.interceptor;

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import java.util.HashMap;
import java.util.Map;

public class GlobalS3V2Interceptor implements ExecutionInterceptor {

    // EXACTLY ONE OVERRIDE METHOD FOR THE ENTIRE LIFECYCLE
    @Override
    public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes) {
        SdkRequest request = context.request();

        // 1. INTERCEPT SINGLE-PASS PUTS (Small file streams)
        if (request instanceof PutObjectRequest) {
            PutObjectRequest putReq = (PutObjectRequest) request;
            String inputPath = executionAttributes.getAttribute(TrackingMetadata.INPUT_FILE_PATH);
            
            if (inputPath != null) {
                Map<String, String> updatedMetadata = new HashMap<>(putReq.metadata());
                updatedMetadata.put("source-metadata-file", inputPath);
                
                // Returns the mutated request builder closure back to the execution engine
                return putReq.toBuilder().metadata(updatedMetadata).build();
            }
        }

        // 2. INTERCEPT MULTIPART INITIALIZATION (Large file streams)
        if (request instanceof CreateMultipartUploadRequest) {
            CreateMultipartUploadRequest mpuReq = (CreateMultipartUploadRequest) request;
            String inputPath = executionAttributes.getAttribute(TrackingMetadata.INPUT_FILE_PATH);
            
            if (inputPath != null) {
                Map<String, String> updatedMetadata = new HashMap<>(mpuReq.metadata());
                updatedMetadata.put("source-metadata-file", inputPath);
                
                // Returns the mutated request builder closure back to the execution engine
                return mpuReq.toBuilder().metadata(updatedMetadata).build();
            }
        }

        // Return unchanged if it's a List or Get request block
        return request;
    }
}
