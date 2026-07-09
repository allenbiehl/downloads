package com.yourcompany.s3.interceptor;

// CORRECT IMPORT: Bypasses the missing ExecutionAttributeKey symbol error
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;

public final class TrackingMetadata {
    
    // AWS SDK v2 uses ExecutionAttribute initialized with a descriptive name string
    public static final ExecutionAttribute<String> INPUT_FILE_PATH = 
        new ExecutionAttribute<>("InputFilePath");
}
