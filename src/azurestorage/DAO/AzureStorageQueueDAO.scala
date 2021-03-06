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

  val baseQueueURL = ".queue.core.windows.net"

  val log = Logger.get
  

  def putMessage( accountName:String, key:String, queueName:String, msg:QueueMessage  ): Status =
  {
    log.info("AzureStorageQueueDAO::putMessage start")
    
    var status = new Status()
 
    var method = new PostMethod(  )
    
    var request = "<QueueMessage><MessageText>"+msg.message+"</MessageText></QueueMessage>"

    var entity = new ByteArrayRequestEntity( request.getBytes  )
    method.setRequestEntity( entity )

    // queueName being manipulated. Need to rename some things here.,
    status = genericSet( method, accountName, key, "", queueName+"/messages", new HashMap[String, String](), request.getBytes )

        
    if (status.code == StatusCodes.CREATE_QUEUE_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }

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
  
  def listQueues( accountName:String, key:String  ): ( Status, List[AzureQueueRef]) =
  {
    log.info("AzureStorageQueueDAO::listQueues start")
    
    var status = new Status()
    var queueList = List[ AzureQueueRef]()
    
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
  
  
  def getMessage( accountName:String, key:String , queueName:String  ): ( Status, Option[ QueueMessage ]) =
  {
    log.info("AzureStorageQueueDAO::listQueues start")
    
    var status = new Status()
    var msg:Option[ QueueMessage ] = None 
        
    var method = new GetMethod(  )
    
    status = genericSet( method, accountName, key, "", queueName+"/messages", new HashMap[String, String](), null )
    
    if (status.code == StatusCodes.GET_MESSAGE_SUCCESS)
    {
      status.successful = true
      
      var responseBody = method.getResponseBodyAsString()

      var xml = responseBody.substring(3)

      log.debug("queue message " + xml )

      var messageList = parseMessage( xml )
      
      // assume only 1 message for now.
      if ( !messageList.isEmpty )
      {
        msg = Option( messageList(0) )
      }

    }
    
    return ( status, msg )
  }    
  // see if I can make a generic set.
  def genericSet( method:HttpMethodBase, accountName:String, key:String, canonicalResourceExtra: String, queueName:String, metaData:HashMap[String, String], data: Array[Byte] ): Status =
  {
    
    log.info("AzureStorageQueueDAO::genericSet start")
    
    var status = new Status()

    // for some reason, the canonicalResourceExtra needs a = for the URL but a : for the canonialResource.
    // go figure....
    var canonicalResource = "/"+accountName+"/"+queueName
    var url = "http://"+accountName+baseQueueURL+"/"+queueName
     
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

  private def parseQueueList( xml: String ): List[ AzureQueueRef ] = 
  {
    log.info("AzureStorageQueueDAO::parseQueueList start")  
    
    var l = List[AzureQueueRef]()
    
    var xmlNode = XML.loadString( xml )
    
    var queueList = xmlNode \\ "Queue"
    
    for ( queue <- queueList )
    {

      var name = ( queue  \ "Name").text
      var q = new AzureQueueRef( name )
      
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




  private def parseMessage( xml: String ): List[QueueMessage] = 
  {
    log.info("AzureStorageQueueDAO::parseMessage start")  
    
    var l = List[QueueMessage]()
    
    var xmlNode = XML.loadString( xml )
    
    var messageList = xmlNode \\ "QueueMessage"
    
    for ( msg <- messageList )
    {

      var text = ( msg  \ "MessageText").text

      var m = new QueueMessage( text )

      m.messageId = ( msg  \ "MessageId").text
     
      // should parse.
      m.insertionTime = ( msg  \ "InsertionTime").text
      
      // should parse
      m.expirationTime = ( msg  \ "ExpirationTime").text

      m.popReceipt = ( msg  \ "PopReceipt").text
     
      // should parse
      m.timeNextVisible = ( msg  \ "TimeNextVisible").text
      
      m.dequeueCount = ( ( msg  \ "DequeueCount").text ).toInt

      log.debug("message is " + m.toString() )
      l ::= m
    }
    
    return l
    
  }
    
        
}
