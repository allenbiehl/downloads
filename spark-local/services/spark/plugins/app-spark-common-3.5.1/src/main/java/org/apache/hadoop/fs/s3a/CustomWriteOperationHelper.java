package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import java.io.IOException;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;

// import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomWriteOperationHelper extends WriteOperationHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomWriteOperationHelper.class);  

  private final S3AFileSystem owner;

  private AuditSpan auditSpan;

  protected CustomWriteOperationHelper(
    S3AFileSystem owner,
    Configuration conf,
    S3AStatisticsContext statisticsContext,
    final AuditSpanSource auditSpanSource,
    final AuditSpan span
  ) {
    super(owner, conf, statisticsContext, auditSpanSource, span);
    this.owner = owner;
    this.auditSpan = span;    
  }

  @Override
  @Retries.RetryTranslated
  public String initiateMultiPartUpload(String destKey) throws IOException {
    LOG.error("########Initiating Multipart upload to {}", destKey);
    try (AuditSpan span = activateAuditSpan()) {
      return retry("initiate MultiPartUpload", destKey, true,
          () -> {
            final InitiateMultipartUploadRequest initiateMPURequest =
                getRequestFactory().newMultipartUploadRequest(
                    destKey);
            injectCustomConfigurations(initiateMPURequest);
            return owner.getAmazonS3Client().initiateMultipartUpload(initiateMPURequest)
                .getUploadId();
          });
    }
  }

  private AuditSpan activateAuditSpan() {
    return auditSpan.activate();
  }

  // @Override
  // @Retries.OnceRaw
  // public com.amazonaws.services.s3.model.PutObjectResult putObject(
  //     com.amazonaws.services.s3.model.PutObjectRequest request, 
  //     org.apache.hadoop.fs.s3a.impl.PutObjectOptions options) throws IOException {
      
  //     return retry("put object", request.getKey(), true, () -> {
  //         // Inject your custom metadata logic into standard put actions here
  //         // Example: request.getMetadata().addUserMetadata("custom-key", "value");
  //         return getOwner().getAmazonS3Client().putObject(request);
  //     });
  // }
  @Override
  @Retries.RetryTranslated
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
      throws IOException {
    LOG.error("########CustomWriteOperationHelper.putObject");  
    return super.putObject(putObjectRequest);
  }


  /**
   * Create a {@link PutObjectRequest} request against the specific key.
   * @param destKey destination key
   * @param inputStream source data.
   * @param length size, if known. Use -1 for not known
   * @param headers optional map of custom headers.
   * @return the request
   */
  @Override
  @Retries.OnceRaw
  public PutObjectRequest createPutObjectRequest(String destKey,
      InputStream inputStream,
      long length,
      final Map<String, String> headers) {
      LOG.error("########CustomWriteOperationHelper.createPutObjectRequest1");          
      return super.createPutObjectRequest(destKey, inputStream, length, headers);
  }

  /**
   * Create a {@link PutObjectRequest} request to upload a file.
   * @param dest key to PUT to.
   * @param sourceFile source file
   * @return the request
   */
  @Override
  @Retries.OnceRaw
  public PutObjectRequest createPutObjectRequest(String dest,
      File sourceFile) {
    LOG.error("########CustomWriteOperationHelper.createPutObjectRequest2");          
    return super.createPutObjectRequest(dest, sourceFile);
  }  

  // /**
  //  * Create a {@link PutObjectRequest} request to upload a file.
  //  * @param dest key to PUT to.
  //  * @param sourceFile source file
  //  * @return the request
  //  */
  // @Retries.OnceRaw
  // public PutObjectRequest createPutObjectRequest(String dest,
  //     File sourceFile) {
  //   Preconditions.checkState(sourceFile.length() < Integer.MAX_VALUE,
  //       "File length is too big for a single PUT upload");
  //   activateAuditSpan();
  //   return getRequestFactory().
  //       newPutObjectRequest(dest,
  //           newObjectMetadata((int) sourceFile.length()),
  //           sourceFile);
  // }

    private void injectCustomConfigurations(InitiateMultipartUploadRequest request) {
        LOG.error("########CustomWriteOperationHelper.injectCustomConfigurations");      
        // Implement your custom metadata modifications or storage properties here.
        // Example: request.setStorageClass("INTELLIGENT_TIERING");
    }
}
