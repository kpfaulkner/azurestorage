/* 
 * Copyright (c) 2009, Ken Faulkner
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.

    * Neither the name of Ken Faulkner nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.
    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package azurestorage.DAO

import azurestorage.Datatypes._


import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params.HttpMethodParams
import org.apache.commons.httpclient.util._
import java.util.Calendar
import org.apache.commons.httpclient.util.DateUtil
import java.util.{Date, GregorianCalendar, TimeZone}
import scala.util.Sorting
import scala.collection.mutable.ListBuffer
import org.apache.commons.httpclient.Header
import java.io.BufferedReader
import java.io.InputStreamReader
import java.security.SignatureException
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64._
import org.apache.commons.codec.binary.Base64
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity
import net.lag.configgy.Configgy
import net.lag.logging.Logger

class AzureStorageBlobDAO 
{

  val baseBlobURL = ".blob.core.windows.net"

  val log = Logger.get
  
  
  // see if I can make a generic set.
  def genericSet( method:HttpMethodBase, accountName:String, key:String, container: String, canonicalResourceExtra: String, blob: Blob ): Status =
  {
    
    log.info("AzureStorageBlobDAO::genericSet start")
    
    var status = new Status()

    // for some reason, the canonicalResourceExtra needs a = for the URL but a : for the canonialResource.
    // go figure....
    var canonicalResource = "/"+accountName+"/"+container+"/"+blob.name +"\n"+canonicalResourceExtra.replace("=",":")
    var url = "http://"+accountName+baseBlobURL+"/"+container+"/"+blob.name + "?"+canonicalResourceExtra
    
    var client = new HttpClient()
    method.setURI( new URI( url ) )
    
    AzureStorageCommon.addMetadataToMethod( method, blob.metaData )
    
    // blob type. 
    // method.setRequestHeader( new Header( BlobProperty.blobType, blob.metaData( BlobProperty.blobType ) ) )
    
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, blob.data )
    
    var res = client.executeMethod( method )    
    var responseBody = method.getResponseBodyAsString()

    log.debug("response body " + responseBody)

    status.code = res
    
    return status
  }
  
  
  def setBlob( accountName:String, key:String, container: String, blob: Blob ): Status =
  {
    log.info("AzureStorageBlobDAO::setBlob start")
    
    var status = new Status()
 
    var method = new PutMethod(  )
    var entity = new ByteArrayRequestEntity( blob.data )
    method.setRequestEntity( entity )
    
    status = genericSet( method, accountName, key, container, "", blob )
    
    if (status.code == StatusCodes.SET_BLOB_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }
  
  def genericGet( method:HttpMethodBase, accountName:String, key:String, container: String, blobName: String ): ( Status, Blob ) =
  {
    log.info("AzureStorageBlobDAO::genericGet start")
    
    var status = new Status()
    
    var canonicalResource = "/"+accountName+"/"+container+"/"+blobName
    var url = "http://"+accountName+baseBlobURL++"/"+container+"/"+blobName
    
    var client = new HttpClient()
    method.setURI( new URI( url ) )
    
    AzureStorageCommon.addMetadataToMethod( method, new HashMap[String,String]() )
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null)
        
    var res = client.executeMethod( method )    
    var h = method.getResponseHeaders()
    var resp = method.getResponseBody()
    
    // for now, just get everything the API gives back. Who am I to filter things?
    var metadata = AzureStorageCommon.extractMetadata( h, "" )
    var blob = new Blob( blobName ) 

    blob.data = resp
    blob.metaData = metadata
 
    status.code = res   
    return ( status, blob )
  }
  
  
  def getBlob( accountName:String, key:String, container: String, blobName: String ): ( Status, Blob ) =
  {
  
    log.info("AzureStorageBlobDAO::getBlob start")
    
    var status = new Status()
    
    var method = new GetMethod( )
    
    var res = genericGet( method, accountName, key, container, blobName  )
    
    status = res._1
    var blob = res._2
    
    
    if (status.code == StatusCodes.GET_BLOB_SUCCESS)
    {
      status.successful = true
    }
    
    return ( status, blob )
  }

  def getBlobProperties( accountName:String, key:String, container: String, blobName: String ): ( Status, Blob ) =
  {
  
    log.info("AzureStorageBlobDAO::getBlobProperties start")
    
    var status = new Status()
    
    var method = new HeadMethod( )
    
    var res = genericGet( method, accountName, key, container, blobName  )
    
    status = res._1
    var blob = res._2
    
    var responseBody = method.getResponseBodyAsString()

    log.debug("response body " + responseBody)
    
    if (status.code == StatusCodes.GET_BLOB_PROPERTIES_SUCCESS)
    {
      status.successful = true
    }
    
    return ( status, blob )
  }

  
  // Get blob metadata
  def getBlobMetadata( accountName:String, key:String, container: String, blobName: String ): ( Status, Blob ) =
  {
  
    log.info("AzureStorageBlobDAO::getBlobMetadata start")
    
    var blob:Blob = null
    var status = new Status()

    var res = getBlobProperties( accountName, key, container, blobName+"comp=metadata")
    
    status = res._1
    blob = res._2
    
    if (res == StatusCodes.GET_BLOB_METADATA_SUCCESS)
    {
      status.successful = true
    } else
    {
      // overwriting the potentially "true" set by getBlobProperties.
      status.successful = false
    }
    return ( status, blob )
  }


  def setBlobProperties( accountName:String, key:String, container:String,  blob:Blob ) : Status =
  {
    
    log.info("AzureStorageBlobDAO::setBlobProperties start")
        
    var status = new Status()

    var method = new PutMethod(  )
    
    status = genericSet( method, accountName, key, container, "comp=properties", blob )
    
    if (status.code == StatusCodes.SET_BLOB_PROPERTIES_SUCCESS)
    {
      status.successful = true
    }
    
    return status

  }
  
  // should reuse setContainerMetadata....
  //def setBlobMetadata( accountName:String, key:String, container:String, blobName:String, keyValuePairs: Map[ String, String] ): Status = 
  def setBlobMetadata( accountName:String, key:String, container:String,  blob:Blob ) : Status =
  {
    
    log.info("AzureStorageBlobDAO::setBlobMetadata start")
    
    
    var status = new Status()
    
    //var blob = new Blob("noidea")
    //blob.metaData = keyValuePairs
    //blob.data = null
    
    var method = new PutMethod(  )
    var entity = new ByteArrayRequestEntity( blob.data )
    method.setRequestEntity( entity )
    
    status = genericSet( method, accountName, key, container, "?comp=metadata", blob )
    
    if (status.code == StatusCodes.SET_BLOB_METADATA_SUCCESS)
    {
      status.successful = true
    }
    
    return status

  }  
  
}
