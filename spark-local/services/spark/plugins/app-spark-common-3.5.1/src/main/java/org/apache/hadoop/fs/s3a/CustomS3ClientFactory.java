package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;
import java.lang.reflect.Field;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomS3ClientFactory extends DefaultS3ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CustomS3ClientFactory.class);

    @Override
    public AmazonS3 createS3Client(URI name, S3ClientFactory.S3ClientCreationParameters parameters) throws IOException {
        // 1. Let the default Hadoop builder construct the immutable client
        AmazonS3 s3Client = super.createS3Client(name, parameters);

        if (s3Client instanceof AmazonS3Client) {
            try {
                // 2. UNLOCK CLIENT IMMUTABILITY VIA REFLECTION
                // This targets the private base flag defined inside com.amazonaws.AmazonWebServiceClient
                Field immutabilityField = com.amazonaws.AmazonWebServiceClient.class.getDeclaredField("isImmutable");
                immutabilityField.setAccessible(true);
                immutabilityField.set(s3Client, false); // Unlock the instance safely!

                LOG.error("######## CustomS3ClientFactory: Successfully unlocked client mutability. Injecting Interceptor. ########");
                System.err.println("######## CustomS3ClientFactory: Successfully unlocked client mutability. Injecting Interceptor. ########");

                // 3. Register the concrete request handler onto the client
                ((AmazonS3Client) s3Client).addRequestHandler(new RequestHandler2() {
                    @Override
                    public void beforeRequest(com.amazonaws.Request<?> request) {
                        Object original = request.getOriginalRequest();
                        
                        if (original != null) {
                            if (original instanceof PutObjectRequest || original instanceof InitiateMultipartUploadRequest) {
                                
                                System.err.println("######## INTERCEPTED UPLOAD AT HTTP LAYER FOR KEY: " + request.getResourcePath() + " ########");
                                
                                // 1. IN-PLACE OBJECT METADATA MUTATION (Fallback/Standard Path)
                                if (original instanceof PutObjectRequest) {
                                    com.amazonaws.services.s3.model.ObjectMetadata metadata = ((PutObjectRequest) original).getMetadata();
                                    if (metadata == null) {
                                        metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
                                        ((PutObjectRequest) original).setMetadata(metadata);
                                    }
                                    metadata.addUserMetadata("custom-staged-by", "my-framework");
                                } else {
                                    com.amazonaws.services.s3.model.ObjectMetadata metadata = ((InitiateMultipartUploadRequest) original).getObjectMetadata();
                                    if (metadata == null) {
                                        metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
                                        ((InitiateMultipartUploadRequest) original).setObjectMetadata(metadata);
                                    }
                                    metadata.addUserMetadata("custom-staged-by", "my-framework");
                                }

                                // 2. THE ULTIMATE BYPASS: Force it directly into the outgoing raw HTTP Request Header Map!
                                // S3 expects custom user metadata via the standard "x-amz-meta-" header naming scheme.
                                // This bypasses Hadoop's object cleansing and sends it straight across the socket wire to MinIO.
                                request.addHeader("x-amz-meta-custom-staged-by", "my-framework");
                                
                                System.err.println("######## Applied HTTP Wire Headers: " + request.getHeaders().toString() + " ########");
                            }
                        }
                    }
                });

                // 4. Relock the client to preserve thread-safety inside Spark's runtime engine
                immutabilityField.set(s3Client, true);

            } catch (Exception e) {
                LOG.error("######## CustomS3ClientFactory: Critical reflection failure tracking the immutability field context", e);
                System.err.println("######## CustomS3ClientFactory: Critical reflection failure tracking the immutability field context");
            }
        }

        return s3Client;
    }
}
