package app.apache.spark.internal.io

import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.Request
import com.amazonaws.services.s3.model.{PutObjectRequest, InitiateMultipartUploadRequest}

class S3RequestInterceptor extends RequestHandler2 {
  
  // FIXED SIGNATURE: Accepts com.amazonaws.Request[_] and returns Unit
  override def beforeRequest(request: Request[_]): Unit = {
    System.err.println("##################################################")
    System.err.println(s"### INTERCEPTED S3 UPLOAD FOR KEY")
    System.err.println("##################################################")    
    val original = request.getOriginalRequest
    
    if (original != null) {
      // Catch single-pass PUT operations (Small files)
      if (original.isInstanceOf[PutObjectRequest]) {
        val putReq = original.asInstanceOf[PutObjectRequest]
        var metadata = putReq.getMetadata
        if (metadata == null) {
          metadata = new com.amazonaws.services.s3.model.ObjectMetadata()
        }
        metadata.addUserMetadata("custom-staged-by", "my-framework")
        putReq.setMetadata(metadata)
      }

      // Catch Multipart sessions (Large files)
      if (original.isInstanceOf[InitiateMultipartUploadRequest]) {
        val mpuReq = original.asInstanceOf[InitiateMultipartUploadRequest]
        var metadata = mpuReq.getObjectMetadata
        if (metadata == null) {
          metadata = new com.amazonaws.services.s3.model.ObjectMetadata()
        }
        metadata.addUserMetadata("custom-staged-by", "my-framework")
        mpuReq.setObjectMetadata(metadata)
      }
    }
  }
}
