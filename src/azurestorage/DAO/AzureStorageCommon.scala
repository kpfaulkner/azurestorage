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

object AzureStorageCommon
{

  val baseBlobURL = ".blob.core.windows.net"

  Configgy.configure("azurestorage.cfg")
  val log = Logger.get
  
  val proxy = Configgy.config.getString("proxy", "")
  val proxyPort =  Configgy.config.getInt("proxy_port", 1080)

  def setupProxy( client: HttpClient ) =
  {
    // just testing out proxy idea. FIXME

    if ( proxy != "" )
    {
      var config = client.getHostConfiguration()
      config.setProxy( proxy, proxyPort )
    }
  }

  def getSortedMSHeaders( headers: Array[Header ]): Array[String] =
  {
    log.info("AzureStorageBlobDAO::getSortedMSHeaders start")
    
    var headerNameList = new ListBuffer[String]()
    
    for ( header <- headers )
    {
      var name = header.getName().toLowerCase()
      headerNameList += name
    }
    
    var ar = headerNameList.toArray[String]
    Sorting.quickSort( ar )
    

    return ar
  }
  

  
  def generateQueryString( method:HttpMethodBase , accountName:String, canonicalResource:String, dataLength:String ): String =
  {
    log.info("AzureStorageBlobDAO::generateQueryString start")
    
    
    var methodName = method.getName()
    var headers = method.getRequestHeaders()
    var headersSorted = getSortedMSHeaders( headers )
    
    var fullUrl = methodName + "\n" + 
               "\n" +
               "\n" +
                dataLength +"\n" +
                "\n"  +
                "\n"  +
                "\n"  +
                "\n" +
                "\n" + 
                "\n" +
                "\n" +
                "\n"
               
    
    for ( header <- headersSorted )
    {
      if ( header.toLowerCase().startsWith("x-ms"))
      {
      
        fullUrl += header.toLowerCase()+":" + method.getRequestHeader( header ).getValue()+"\n"
      }
    }
    
    //fullUrl += canonicalResource
    fullUrl += canonicalResource

    log.debug("query string to encode "+ fullUrl )
    return fullUrl
  }
    
  // create partial function with URL generator?
  def generateHMAC( method:HttpMethodBase, key:String, accountName:String, canonicalResource:String, dataLength:String): String =
  {
    log.info("AzureStorageBlobDAO::generateHMAC start")
    var signingKey = new SecretKeySpec( decodeBase64( key ), "HmacSHA256")

    var mac = Mac.getInstance("HmacSHA256")   
    mac.init( signingKey )
    
    var url = generateQueryString( method, accountName, canonicalResource, dataLength)
    var rawHmac = mac.doFinal(url.getBytes());
    var result = encodeBase64String(rawHmac)
    var result2 = result.trim()
    result = result2
        
    return result
    
  }
  
  def getUTC(): String =
  {

    var calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"))

    var now = calendar.getTime()
    
    var dateHeader = DateUtil.formatDate( now, "EEE, dd MMM yyyy HH:mm:ss ") + "GMT"    
    
    return dateHeader
  }
  
  
  def populateMethod( method:HttpMethodBase, key:String, accountName:String, canonicalResource: String, data:Array[Byte]): Status =
  {
    log.info("AzureStorageCommon::populateMethod start")
    var status = new Status()
    
    var dateHeader = getUTC()

    var lengthStr = ""
    if ( data != null )
    {
      lengthStr = data.length.toString()
      
    }
    
    var headers = method.getRequestHeaders()
    method.setRequestHeader( new Header("x-ms-date", dateHeader) )
    method.setRequestHeader( new Header("x-ms-version", "2009-09-19" ) )
 
    var myHash = generateHMAC( method, key, accountName, canonicalResource, lengthStr)
    var authorization = "SharedKey "+accountName+":"+ myHash 
   
    method.setRequestHeader( new Header("Authorization", authorization) )
    
    return status
  }

  def addMetadataToMethod( method:HttpMethodBase, keyValuePairs: Map[ String, String]): Status =
  {

    var status= new Status()
    
    if (keyValuePairs != null )
    {
      for ( kv <- keyValuePairs )
      {
        var header = kv._1
        // only add the prefix if we dont already start with X.
        if ( ! kv._1.toLowerCase().startsWith("x") )
        {
          header = "X-ms-meta-" + header
        }
        
        method.setRequestHeader( new Header( header , kv._2) )
      } 
    }
    return status
  }
  
  def extractMetadata( headers: Array[ Header ], prefix:String ): Map[String, String ] =
  {
  
    var l = new ListBuffer[ Tuple2[String, String ]]()
    
    var hm = Map[String, String]()
    
    for (h <- headers )
    {
      if ( h.getName().toLowerCase().startsWith(prefix) )
      {

        hm(  h.getName().substring( prefix.length ) ) = h.getValue()
        
      }
    }
    
    
    return hm
    
  }
  
}
