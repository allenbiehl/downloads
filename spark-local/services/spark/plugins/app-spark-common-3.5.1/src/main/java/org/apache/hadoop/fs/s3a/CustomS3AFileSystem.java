package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;

import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.Progressable;

public class CustomS3AFileSystem extends S3AFileSystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomWriteOperationHelper.class);    


  @Override
  @Retries.OnceRaw
  public UploadInfo putObject(PutObjectRequest putObjectRequest) {
    LOG.error("###CustomS3AFileSystem.putObject");     
    return super.putObject(putObjectRequest);
  }


  @Override
  @Retries.OnceRaw("For PUT; post-PUT actions are RetryTranslated")
  UploadResult executePut(PutObjectRequest putObjectRequest,
      Progressable progress
  ) throws InterruptedIOException, MetadataPersistenceException {
      LOG.error("###CustomS3AFileSystem.executePut");      
      injectCustomConfigurations(putObjectRequest);
        
        // 2. Delegate back to the native S3 client pipeline
      return super.executePut(putObjectRequest, progress);
  }    

    private void injectCustomConfigurations(PutObjectRequest request) {
      LOG.error("###CustomS3AFileSystem.injectCustomConfigurations");      
        com.amazonaws.services.s3.model.ObjectMetadata metadata = request.getMetadata();
        if (metadata == null) {
            metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
        }
        // Example logic
        metadata.addUserMetadata("custom-staged-by", "my-framework");
        request.setMetadata(metadata);
    }    

    @Override
    @InterfaceAudience.Private
    public WriteOperationHelper createWriteOperationHelper(AuditSpan auditSpan) {
      LOG.error("###createWriteOperationHelper");
        try {
            // Extract the private statisticsContext field using reflection
            Field statsField = S3AFileSystem.class.getDeclaredField("statisticsContext");
            statsField.setAccessible(true);
            S3AStatisticsContext statsContext = (S3AStatisticsContext) statsField.get(this);

            // Instantiate your custom helper with the exact 5 parameters it expects
            return new CustomWriteOperationHelper(
                this,
                getConf(),
                statsContext,
                getAuditSpanSource(),
                auditSpan
            );
        } catch (Exception e) {
            // Fallback strategy if reflection is restricted by the JVM classloader environment
            throw new RuntimeException("Failed to extract private statisticsContext field via reflection", e);
        }          
    }    
}