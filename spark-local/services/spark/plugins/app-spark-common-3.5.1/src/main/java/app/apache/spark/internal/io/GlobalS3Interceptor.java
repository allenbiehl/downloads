package app.apache.spark.internal.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;

public class GlobalS3Interceptor extends RequestHandler2 {
  private static final Logger LOG =
      LoggerFactory.getLogger(GlobalS3Interceptor.class);    

    // Explicit public constructor to capture the exact millisecond the AWS SDK loads your file
    public GlobalS3Interceptor() {
        System.err.println("############################################################");
        System.err.println("### GlobalS3Interceptor: INITIALIZED VIA AWS SDK REFLECTION ###");
        System.err.println("############################################################");
    }

    @Override
    public void beforeRequest(com.amazonaws.Request<?> request) {
        LOG.error("######## Before Request. ########");
        Object original = request.getOriginalRequest();
        
        if (original != null) {
            LOG.error("######## original {}. ########", original.getClass());          
            // Target the two primary write vectors used by Spark and the Staging Committer
            if (original instanceof PutObjectRequest || original instanceof InitiateMultipartUploadRequest) {
                LOG.error("######## Matched request type. Adding headers. ########");              
                
                // Pure HTTP wire injection bypasses all Hadoop object-cleansing pipelines
                request.addHeader("x-amz-meta-custom-staged-by", "my-framework");
            }
        }
    }
}
