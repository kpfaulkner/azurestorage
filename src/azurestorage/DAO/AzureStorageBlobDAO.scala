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
import scala.xml.XML
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
  def genericSet( method:HttpMethodBase, accountName:String, key:String, container: String, canonicalResourceExtra: String, name:String, metaData:HashMap[String, String], data: Array[Byte] ): Status =
  {
    
    log.info("AzureStorageBlobDAO::genericSet start")
    
    var status = new Status()

    // for some reason, the canonicalResourceExtra needs a = for the URL but a : for the canonialResource.
    // go figure....
    var canonicalResource = "/"+accountName+"/"+container+"/"+name 
    var url = "http://"+accountName+baseBlobURL+"/"+container+"/"+name 
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
  
  
  def putBlob( accountName:String, key:String, container: String, blob: Blob ): Status =
  {
    log.info("AzureStorageBlobDAO::putBlob start")
    
    var status = new Status()
 
    var method = new PutMethod(  )
    var entity = new ByteArrayRequestEntity( blob.data )
    method.setRequestEntity( entity )
    
    status = genericSet( method, accountName, key, container, "", blob.name, blob.metaData, blob.data )
    
    if (status.code == StatusCodes.SET_BLOB_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }
  
  def genericGet( method:HttpMethodBase, accountName:String, key:String, container: String, blobName: String ,canonicalResourceExtra: String): ( Status, Blob ) =
  {
    log.info("AzureStorageBlobDAO::genericGet start")
    
    var status = new Status()
    
    //var canonicalResource = "/"+accountName+"/"+container+"/"+blobName
    //var url = "http://"+accountName+baseBlobURL++"/"+container+"/"+blobName
    var canonicalResource = "/"+accountName+"/"+container
    var url = "http://"+accountName+baseBlobURL+"/"+container

    if (blobName != null && blobName != "")
    {
      canonicalResource += "/"+blobName
      url += "/" + blobName
    }

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
    
    var res = genericGet( method, accountName, key, container, blobName,""  )
    
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
    
    var res = genericGet( method, accountName, key, container, blobName,""  )
    
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
    
    var status = new Status()
    
    var method = new HeadMethod( )
    
    var res = genericGet( method, accountName, key, container, blobName,"comp=metadata"  )
    
    status = res._1
    var blob = res._2
    
    var responseBody = method.getResponseBodyAsString()

    log.debug("response body " + responseBody)
    
    if (status.code == StatusCodes.GET_BLOB_METADATA_SUCCESS)
    {
      status.successful = true
    }
    
    return ( status, blob )
  }


  def setBlobProperties( accountName:String, key:String, container:String,  blob:Blob ) : Status =
  {
    
    log.info("AzureStorageBlobDAO::setBlobProperties start")
        
    var status = new Status()

    var method = new PutMethod(  )
    
    status = genericSet( method, accountName, key, container, "", blob.name, blob.metaData, blob.data )
    
    if (status.code == StatusCodes.SET_BLOB_PROPERTIES_SUCCESS)
    {
      status.successful = true
    }
    
    return status

  }

  def deleteBlob( accountName:String, key:String, container:String,  blobName:String) : Status =
  {
    
    log.info("AzureStorageBlobDAO::deleteBlob start")
        
    var status = new Status()
    var method = new DeleteMethod(  )

    // for some reason, the canonicalResourceExtra needs a = for the URL but a : for the canonialResource.
    // go figure....
    //var canonicalResource = "/"+accountName+"/"+container+"/"+blob.name +"\n"+canonicalResourceExtra.replace("=",":")
    //var url = "http://"+accountName+baseBlobURL+"/"+container+"/"+blob.name + "?"+canonicalResourceExtra
    var canonicalResource = "/"+accountName+"/"+container+"/"+blobName 
    var url = "http://"+accountName+baseBlobURL+"/"+container+"/"+blobName 


    var client = new HttpClient()
    method.setURI( new URI( url ) )
    
    AzureStorageCommon.addMetadataToMethod( method, new HashMap[String, String]() )
    
    // blob type. 
    // method.setRequestHeader( new Header( BlobProperty.blobType, blob.metaData( BlobProperty.blobType ) ) )
    
    AzureStorageCommon.populateMethod( method, key, accountName, canonicalResource, null )

    // setup proxy.
    AzureStorageCommon.setupProxy( client )    
    var res = client.executeMethod( method )    
    var responseBody = method.getResponseBodyAsString()

    log.debug("response body " + responseBody)

    status.code = res
    
    if (status.code == StatusCodes.DELETE_BLOB_SUCCESS)
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
    
    status = genericSet( method, accountName, key, container, "comp=metadata", blob.name, blob.metaData, blob.data )
    
    if (status.code == StatusCodes.SET_BLOB_METADATA_SUCCESS)
    {
      status.successful = true
    }
    
    return status

  }  

  def listBlobs( accountName:String, key:String, containerName:String  ): ( Status, List[Blob] ) = 
  {
    log.info("AzureStorageBlobDAO::listBlobs start")  
    
    var url = "http://"+accountName+baseBlobURL+"/?restype=container&include=metadata&comp=list"
    var result = List[Blob]()
    var status = new Status()

    var method = new GetMethod( )
    
    var res = genericGet( method, accountName, key, containerName, "","comp=list&include=metadata&restype=container"  )

    status = res._1

    var responseBody = method.getResponseBodyAsString()

    var xml = responseBody.substring(3)

    var blobList = parseBlobList( xml )
 
    if (status.code == StatusCodes.GET_BLOB_LIST_SUCCESS)
    {
      status.successful = true
    }

    return (status, blobList )

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

  private def parseBlobList( xml: String ): List[Blob] = 
  {
    log.info("AzureStorageBlobDAO::parseBlobList start")  
    
    var l = List[Blob]()
    
    var xmlNode = XML.loadString( xml )
    
    var blobList = xmlNode \\ "Blob"
    
    for ( blob <- blobList )
    {

      // dont bother with last mod time or etag *yet*
      var name = (blob \ "Name").text
      var b = new Blob( name )
      
      // get properties
      var properties = ( blob \\ "Properties")
      var propertiesHM = hackyParseTags( properties.toString() )

      // get metadata
      var metadata = ( blob \\ "Metadata")
      var metadataHM = hackyParseTags( metadata.toString() )
      
      // merge
      metadataHM ++ propertiesHM
      b.metaData = metadataHM

      // l += b scala 2.7
      l ::= b  // scala 2.8
    }
    
    return l
    
  }

  def putBlock( accountName:String, key:String, container: String, block: Block ): Status =
  {
    log.info("AzureStorageBlobDAO::putBlock start")
    
    var status = new Status()
 
    var method = new PutMethod(  )
    var entity = new ByteArrayRequestEntity( block.data )
    method.setRequestEntity( entity )
    
    var urlExtra = "comp=block&blockid="+ block.status.blockId

    status = genericSet( method, accountName, key, container, urlExtra, block.blobName, null, block.data )
    
    if (status.code == StatusCodes.PUT_BLOCK_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }

  def generateBlockList( blockList: List[ BlockStatus]): String =
  {
    log.info("AzureStorageBlobDAO::generateBlockList start")
    
    var s = "<BlockList>"

    for ( i <- blockList )
    {
      s += "<"+i.statusCode+">"+i.blockId+"</"+i.statusCode+">"
    }

    s += "</BlockList>"

    return s

  }


  // put block list.
  // cover blob holds the information about the blob as a whole once its formed.
  // eg all the propreties and metadata stuff.
  def putBlockList( accountName:String, key:String, container: String, blobName:String, blockList: List[ BlockStatus], metaData: HashMap[ String, String] ): Status =
  {
    log.info("AzureStorageBlobDAO::putBlockList start")
    
    var status = new Status()
 
    var method = new PutMethod(  )

    // make sure the content type.
    metaData("Content-Type") = "text/plain; charset=UTF-8"

    // generated list of blocks.
    var blockInfo = generateBlockList( blockList )

    // block info....
    var entity = new ByteArrayRequestEntity( blockInfo.getBytes() )
    method.setRequestEntity( entity )
    
    var urlExtra = "comp=blocklist"

    status = genericSet( method, accountName, key, container, urlExtra, blobName, metaData, blockInfo.getBytes() )
    
    if (status.code == StatusCodes.PUT_BLOCK_SUCCESS)
    {
      status.successful = true
    }
    
    return status
  }
  
  def getBlockList( container: String, blobName: String ): ( Status, Array[ String] ) =
  {
    return (null, null)
  }

}
