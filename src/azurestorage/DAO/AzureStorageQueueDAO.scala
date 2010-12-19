/* 
 * Copyright (c) 2010, Ken Faulkner
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
import scala.xml.XML
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64._
import org.apache.commons.codec.binary.Base64
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity
import net.lag.configgy.Configgy
import net.lag.logging.Logger

class AzureStorageQueueDAO 
{

  val baseBlobURL = ".queue.core.windows.net"

  val log = Logger.get
  

  def createQueue( accountName:String, key:String, queueName:String ): Status =
  {
    log.info("AzureStorageQueueDAO::createQueue start")
    
    var status = new Status()
 
    var method = new PutMethod(  )
    
    status = genericSet( method, accountName, key, "", queueName, new HashMap[String, String](), null )
    
    if (status.code == StatusCodes.CREATE_QUEUE_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }
  
  def listQueues( accountName:String, key:String  ): ( Status, List[AzureQueue]) =
  {
    log.info("AzureStorageQueueDAO::listQueues start")
    
    var status = new Status()
    var queueList = List[ AzureQueue]()
    
    var method = new GetMethod(  )
    
    status = genericSet( method, accountName, key, "comp=list", "", new HashMap[String, String](), null )
    
    if (status.code == StatusCodes.LIST_QUEUES_SUCCESS)
    {
      status.successful = true
      
      var responseBody = method.getResponseBodyAsString()

      var xml = responseBody.substring(3)

      queueList = parseQueueList( xml )
      
    }
    
    return ( status, queueList )
  }
  
  
    
  // see if I can make a generic set.
  def genericSet( method:HttpMethodBase, accountName:String, key:String, canonicalResourceExtra: String, queueName:String, metaData:HashMap[String, String], data: Array[Byte] ): Status =
  {
    
    log.info("AzureStorageQueueDAO::genericSet start")
    
    var status = new Status()

    // for some reason, the canonicalResourceExtra needs a = for the URL but a : for the canonialResource.
    // go figure....
    var canonicalResource = "/"+accountName+"/"+queueName
    var url = "http://"+accountName+baseBlobURL+"/"+queueName
     
    if ( canonicalResourceExtra != "" )
    {
      canonicalResource += "\n"+canonicalResourceExtra.replace("=",":")
      url += "?"+canonicalResourceExtra
    }

    var client = new HttpClient()
    method.setURI( new URI( url ) )
    
    AzureStorageCommon.addMetadataToMethod( method, metaData )
    
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, data )
    // setup proxy.
    AzureStorageCommon.setupProxy( client )    
    var res = client.executeMethod( method )    
    var responseBody = method.getResponseBodyAsString()

    log.debug("response body " + responseBody)

    status.code = res
    
    return status
  }

  private def hackyParseTags( xml: String ): HashMap[String, String ] =
  {
    log.debug("hackyParseTags start" )

    var hm = new HashMap[String, String]()

    if ( xml != "")
    {
      var e = XML.loadString( xml )
      
      for ( child <- e.child )
      {
        var tag = child.label
        var data = child.text
        hm( tag ) = data
      }
    }

    return hm
  }

  private def parseQueueList( xml: String ): List[ AzureQueue ] = 
  {
    log.info("AzureStorageQueueDAO::parseQueueList start")  
    
    var l = List[AzureQueue]()
    
    var xmlNode = XML.loadString( xml )
    
    var queueList = xmlNode \\ "Queue"
    
    for ( queue <- queueList )
    {

      var name = ( queue  \ "Name").text
      var q = new AzureQueue( name )
      
      var url = ( queue  \ "Url").text
      q.url = url
      
      // get metadata
      var metaData = ( queue \\ "Metadata")
      var metaDataHM = hackyParseTags( metaData.toString() )
      q.metaData = metaDataHM

      l ::= q
    }
    
    return l
    
  }
    
}
