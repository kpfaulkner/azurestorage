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
import org.apache.commons.httpclient.util.DateUtil
import scala.collection.mutable.ListBuffer
import org.apache.commons.httpclient.Header
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.commons.codec.binary.Base64._
import org.apache.commons.codec.binary.Base64
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.xml.XML
import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time.DateTime
import org.joda.time.DateTimeZone



class AzureStorageContainerDAO 
{

  val baseBlobURL = ".blob.core.windows.net"
    
  val log = Logger.get



  private def parseContainerList( xml: String ): List[Container] = 
  {
    log.info("AzureStorageContainerDAO::parseContainerList start")  
    
    var l = List[Container]()
    
    var xmlNode = XML.loadString( xml )
    
    var containerList = xmlNode \\ "Container"
    
    for ( container <- containerList )
    {
    
      // dont bother with last mod time or etag *yet*
      var c = new Container()
      c.name = (container \ "Name").text
      c.url = ( container \ "Url").text
     
      //l += c  2.7
      l ::= c
    }
    
    return l
    
  }
  
  
  def listContainers( accountName:String, key:String ): ( Status, List[Container] ) = 
  {
    log.info("AzureStorageContainerDAO::listContainers start")  
    
    var url = "http://"+accountName+baseBlobURL+"/?comp=list"
    var result = List[Container]()

    var client = new HttpClient()   
    var method = new GetMethod( url )
    var canonicalResource = "/"+accountName+"/?comp=list"
  
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null )
    
    // setup proxy.
    AzureStorageCommon.setupProxy( client )

    var status = new Status()
    
    var res = client.executeMethod( method )    
    
    if ( res == StatusCodes.LIST_CONTAINERS_SUCCESS )
    {
      status.successful = true
    }
    
    var responseBody = method.getResponseBody()
    
    // warning magic magic magic!!!  but seems to be a bug where I get crap at the beginning of the response!
    var subArray = responseBody.drop(3)
    var subsubArray = subArray.toArray
    var subArrayAsString = new String( subsubArray )
    result = parseContainerList(subArrayAsString)
    return (status, result )
  }
  
  



  def createContainer( accountName:String, key:String, container:String ): Status = 
  {
    log.info("AzureStorageContainerDAO::createContainer start") 
    var url = "http://"+accountName+baseBlobURL+"/"+container+"?restype=container"
  
    var status = new Status()

    var client = new HttpClient()   
    var method = new PutMethod( url )
    var canonicalResource = "/"+accountName+"/" + container + "\nrestype:container"
    //var canonicalResource = "/"+accountName+"/" + container + "\n"

    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null )

    // setup proxy.
    AzureStorageCommon.setupProxy( client )

    var res = client.executeMethod( method )    
     
    log.debug("res is " + res.toString() )

    status.code = res
    var responseBody = method.getResponseBodyAsString()

    log.debug("response body " + responseBody)

    if (res == StatusCodes.CREATE_CONTAINER_SUCCESS )
    {
      status.successful = true
    }
    
    return status
  }

  def deleteContainer( accountName:String, key:String, container:String ): Status = 
  {
    log.info("AzureStorageContainerDAO::deleteContainer start") 
    var url = "http://"+accountName+baseBlobURL+"/"+container
  
    var status = new Status()

    var client = new HttpClient()   
    var method = new DeleteMethod( url )
    var canonicalResource = "/"+accountName+"/" + container
  
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null )
    // setup proxy.
    AzureStorageCommon.setupProxy( client )

    var res = client.executeMethod( method )    
    status.code = res 
   
    if (res == StatusCodes.DELETE_CONTAINER_SUCCESS )
    {
      status.successful = true
    }
    
      
    return status
  }
    

  
  def setContainerMetadata( accountName:String, key:String, container:String, keyValuePairs: Map[ String, String] ): Status = 
  {
    
    log.info("AzureStorageContainerDAO::setContainerMetadata start") 
    var canonicalResource = "/"+accountName+"/"+container+"?comp=metadata"
    var url = "http://"+accountName+baseBlobURL+"/"+container+"?restype=container&comp=metadata"
    var status = new Status()
    var client = new HttpClient()    
    var method = new PutMethod( url )
    
    keyValuePairs += "x-ms-version" -> "2009-09-19"
    
    AzureStorageCommon.addMetadataToMethod( method, keyValuePairs )
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null )
    
    // setup proxy.
    AzureStorageCommon.setupProxy( client )
         
    var res = client.executeMethod( method )    
    var responseBody = method.getResponseBodyAsString()
    
    status.code = res
    
    if (res == StatusCodes.SET_CONTAINER_METADATA_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }

  
  def getContainerMetadata( accountName:String, key:String, container:String  ): ( Status, Map[String, String]) = 
  {
    log.info("AzureStorageContainerDAO::getContainerMetadata start") 
    var canonicalResource = "/"+accountName+"/"+container
    var url = "http://"+accountName+baseBlobURL+canonicalResource
    var status = new Status()
    var client = new HttpClient()
  
    var method = new GetMethod( url )
    
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null )
    
    // setup proxy.
    AzureStorageCommon.setupProxy( client )
             
    var res = client.executeMethod( method )    
    var h = method.getResponseHeaders()
    var metadata = AzureStorageCommon.extractMetadata( h, "" )
    
    status.code = res
    
    if (res == StatusCodes.GET_CONTAINER_METADATA_SUCCESS)
    {
      status.successful = true
    }
    
    return ( status, metadata)
  }

  def generateDateString( d:Date ): String =
  {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    
    var newDate = format.format( d ) + "Z"
    
    return newDate
    
  }
  
  def generateACLPermission( acl:ContainerACL ): String =
  {
  
    var perm = ""
    
    // figure out the scala shorthand for this later.
    if ( acl.canRead )
    {
       perm += "r"
    }
    
    if ( acl.canWrite )
    {
       perm += "w"
    }
    
    if ( acl.canDelete )
    {
       perm += "d"
    }
    
    return perm
    
  }
  
  def generateACLXML( ACLList:List[ ContainerACL  ] ): String =
  {
    var xml = <SignedIdentifiers>
              { for (acl <- ACLList ) yield 
                <SignedIdentifier> 
                  <Id>{acl.uid}</Id>
                  <AccessPolicy>
                    <Start>{ generateDateString( acl.startTime )}</Start>
                    <Expiry>{generateDateString( acl.endTime )}</Expiry>
                    <Permission>{generateACLPermission( acl ) }</Permission>
                  </AccessPolicy>
                </SignedIdentifier>
              }
              </SignedIdentifiers>

              
    return xml.toString()

  }

  // see if I can make a generic set.
  def genericSet( method:HttpMethodBase, accountName:String, key:String, container: String, canonicalResourceExtra: String, metaData:HashMap[String, String], data: Array[Byte] ): Status =
  {
    
    log.info("AzureStorageBlobDAO::genericSet start")
    
    var status = new Status()

    // for some reason, the canonicalResourceExtra needs a = for the URL but a : for the canonialResource.
    // go figure....
    var canonicalResource = "/"+accountName+"/"+container
    var url = "http://"+accountName+baseBlobURL+"/"+container 
    if ( canonicalResourceExtra != "" )
    {
      canonicalResource += "\n"+canonicalResourceExtra.replace("=",":").replace("&","\n")
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
    
  def setContainerACL( accountName:String, key:String, containerName:String, ACLList:List[ ContainerACL ], publicAccess:Boolean ): Status = 
  {
    
    log.info("AzureStorageContainerDAO::setContainerACL start") 
    var status = new Status()
    
    var metaData = new HashMap[String, String]()
    var isPublic = "true"
    
    if ( !publicAccess )
    {
      isPublic = "false"
      
    }
    
    var xml = generateACLXML( ACLList )
    
    log.debug("XML is " + xml )
    
    var method = new PutMethod( )
    
    metaData("x-ms-prop-publicaccess") = isPublic
    
    var entity = new ByteArrayRequestEntity( xml.getBytes() )
    method.setRequestEntity( entity )

    status = genericSet( method, accountName, key, containerName, "comp=acl&restype=container",metaData, xml.getBytes() )
    
    if ( status.code == StatusCodes.SET_CONTAINER_ACL_SUCCESS )
    {
      status.successful = true
    }
    
    var responseBody = method.getResponseBodyAsString()
    log.debug("set container acl response " + responseBody )
    
    return status
  }

  def parseACLXML( xmlStr:String ): List[ ContainerACL] =
  {
    
    log.info("parseACLXML start")
    log.debug("xml string is "+ xmlStr )
    
    var xml = XML.loadString( xmlStr )
    
    log.debug("have xml")
    
    var aclList = (xml \\ "SignedIdentifier")
    
    var l = List[ContainerACL]()
    
    for (acl <- aclList )
    {
    
      log.debug("in acl loop")
      var realACL = new ContainerACL()
      realACL.uid = ( acl \\ "Id").text
      
      var startTime = ( acl \\ "Start").text
      var endTime = ( acl \\ "Expiry").text
      
      var realStartTime = new DateTime( startTime ).withZone(DateTimeZone.UTC).toDate()
      var realEndTime = new DateTime( endTime ).withZone(DateTimeZone.UTC).toDate()
      
      realACL.startTime = realStartTime
      realACL.endTime = realEndTime
      
      var perms = ( acl \\ "Permission").text
      
      if ( perms.exists( _ == 'r' ) )
      {
        realACL.canRead = true
      } else {
        realACL.canRead = false
      }
      
      if ( perms.exists( _ == 'w' ) )
      {
        realACL.canWrite = true
      } else {
        realACL.canWrite = false
      }
      
      
      if ( perms.exists( _ == 'd' ) )
      {
        realACL.canDelete = true
      } else {
        realACL.canDelete = false
      }
      
      // l += realACL 2.7
      l ::= realACL
      
    }
    
    return( l )
    
  }
  
  def genericGet( method:HttpMethodBase, accountName:String, key:String, container: String ,canonicalResourceExtra: String): Status =
  {
    log.info("AzureStorageBlobDAO::genericGet start")
    
    var status = new Status()
    
    var canonicalResource = "/"+accountName+"/"+container
    var url = "http://"+accountName+baseBlobURL+"/"+container

    if ( canonicalResourceExtra != "" )
    {
      canonicalResource += "\n"+canonicalResourceExtra.replace("=",":").replace("&","\n")
      url += "?"+canonicalResourceExtra
    }

    var client = new HttpClient()
    method.setURI( new URI( url ) )
    
    AzureStorageCommon.addMetadataToMethod( method, new HashMap[String,String]() )
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null)
    
    // setup proxy.
    AzureStorageCommon.setupProxy( client )
        
    var res = client.executeMethod( method )    
   
    status.code = res   
    return status
  }

  
  def getContainerACL( accountName:String, key:String, container:String ): (Status, List[ ContainerACL ] ) = 
  {
    
    log.info("AzureStorageContainerDAO::getContainerACL start") 
    
    var status = new Status()
    
    var method = new GetMethod( )
    
    status = genericGet( method, accountName, key, container,"comp=acl&restype=container"  )
    
    if ( status.code == StatusCodes.GET_CONTAINER_ACL_SUCCESS)
    {
      status.successful = true
    }
    
    var h = method.getResponseHeaders()  
    
    var responseBody = method.getResponseBody()
    var subArray = responseBody.drop(3)
    var subsubArray = subArray.toArray
    var rb2 = new String( subsubArray )
    
    //var resp = method.getResponseBody()
    log.debug("get container acl response " + rb2 )
    var l = parseACLXML( rb2 )
    
    return ( status, l)
  }
    
  
}
