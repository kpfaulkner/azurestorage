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

package azurestorage

import azurestorage.Datatypes._
import azurestorage._
import azurestorage.DAO._
import scala.collection.mutable._
import scala.collection.mutable.Map
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import java.io.FileOutputStream
import java.io.FileInputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import org.apache.commons.io.FilenameUtils

// This is an extension of the REST API provided for AzureStorage. (well eventually will be )
// For example, instead of just storing blobs where blobs are already created instances of Blob, we can provide a wrapper method 
// that takes a filepath etc.
// basically all the extras will just be convenience wrappers.
class AzureBlobStorage( accName:String, k: String )
{
  
  var key = k
  var accountName = accName
  
  var blobDao = new AzureStorageBlobDAO()
  var containerDao = new AzureStorageContainerDAO()
  
  Configgy.configure("azurestorage.cfg")
  val log = Logger.get
  
  def createContainer(  container:String ): Status = 
  {
    var status = new Status()
  
    try
    {
      status = containerDao.createContainer( accountName, key, container)
    
    }
    catch 
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::createContainer exception " + ex.toString() )
          status.code = StatusCodes.FAILED
        }
    }
    
    
    return status
  }

  def listContainers(  ): ( Status, List[Container] ) = 
  {
    var status = new Status()
    var l = List[Container]()
    
    try
    {
      var res  = containerDao.listContainers( accountName, key )
      
      status = res._1
      l = res._2
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::listContainers exception")
          status.code = StatusCodes.FAILED
        }
    }
    
    return (status, l )
  }
    
  def deleteContainer(  container:String ): Status = 
  {
    
    var status = new Status()
    
    try
    {
      containerDao.deleteContainer( accountName, key, container)
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::deleteContainer exception")
          status.code = StatusCodes.FAILED
        }
    }
    
    return status
  }  
  
  def setContainerMetadata(  container:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var header = Map[String, String]()
      header( metaName ) = metaValue
    
      containerDao.setContainerMetadata( accountName, key, container, header )

    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setContainerMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return status
  }  

  def setContainerMetadata(  container:String,  keyValuePairs: Map[ String, String ] ): Status = 
  {
    
    var status = new Status()

    try
    {
      containerDao.setContainerMetadata( accountName, key, container, keyValuePairs )
 
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setContainerMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }        
    
    return status
  }  
  
  def addContainerMetadata(  container:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var resp = containerDao.getContainerMetadata( accountName, key, container )
    
      var headers = resp._2
    
      headers( metaName ) = metaValue
    
      containerDao.setContainerMetadata( accountName, key, container, headers )
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::addContainerMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }        
    return status
  }    
    
  def getContainerMetadata(  container:String ): ( Status, Map[String, String]  ) = 
  {
    
    var status = new Status()

    var metadata:Map[String, String] = null
    
    try
    {
      var resp = containerDao.getContainerMetadata( accountName, key, container)
      status = resp._1
      metadata = resp._2

    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::getContainerMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return ( status, metadata )
  }  
  
  
  // filename is fully qualified.
  // destination is the location within the container.
  def setBlobByFilename( container: String, filename: String, destination:String ): Status =
  {
    var status = new Status()
    
    try
    {
      var inFile = new FileInputStream( filename )
      
      var buffer = new Array[Byte](1024)
      var len = 0
      var baos = new ByteArrayOutputStream()
      var pos = 0
      var done = false
      while  ( !done )
      {
        len = inFile.read( buffer )
  
        if ( len > 0 )
        {
          baos.write( buffer, 0, len )
          pos += len
        }
        else
        {
          done = true 
        }
      }
     
        
      var basename = FilenameUtils.getName( filename )        

      // blob name will just be the actual filename
      //var blob = new Blob( basename )
      var blob = new Blob( destination )      
      blob.data = baos.toByteArray()
        
      status = blobDao.setBlob( accountName, key, container, blob )

    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setBlobByFilePath exception " + ex.toString() )
          status.code = StatusCodes.FAILED
          status.message = "Failed to set blob by filepath"
        }
    }  
    
    return status
  }
  
  // put a block. 
  // 
  def putBlock( container: String, block: Block ): Status =
  {
    
  }

  // put block list.
  def putBlockList( container: String, blockList: Array[ String ], coverBlob:Blob ): Status =
  {
    
  }
  
  def getBlockList( container: String, blobName: String ): ( Status, Array[ String] ) =
  {
    
  }
    
  def putBlob( container: String, blob: Blob ): Status =
  {
    var status = new Status()
    
    try
    {
      status = blobDao.putBlob( accountName, key, container, blob )

    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::putBlob exception")
          status.code = StatusCodes.FAILED
        }
    }  
    
    return status
  }

  def deleteBlob( container: String, blobName: String ):  Status  =
  {
  
    
    var status = new Status()
    
    try
    {
      status = blobDao.deleteBlob( accountName, key, container, blobName )
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::deleteBlob exception")
          status.code = StatusCodes.FAILED
        }
    }  
    
    return status
  }
    
  def getBlob( container: String, blobName: String ): ( Status, Blob ) =
  {
  
    var blob:Blob = null
    
    var status = new Status()
    
    try
    {
      var res = blobDao.getBlob( accountName, key, container, blobName )
      status = res._1
      blob = res._2
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::getBlob exception")
          status.code = StatusCodes.FAILED
        }
    }  
    
    return ( status, blob )
  }

  // set container ACL via container string name.
  def setContainerACL( container: String, ACLList:List[ ContainerACL ], isPublic:boolean ): Status =
  {
    var status = new Status()
    
    try
    {
      status = containerDao.setContainerACL( accountName, key, container, ACLList, isPublic )

    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setBlob exception")
          status.code = StatusCodes.FAILED
        }
    }  
    
    return status
  }
  
  def getContainerACL( container:String ): (Status, List[ ContainerACL ] ) = 
  {
    var status = new Status()
    
    var l:List[ ContainerACL] = null
    
    try
    {
      var res = containerDao.getContainerACL( accountName, key, container )

      status = res._1
      l = res._2
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::getContainerACL exception")
          status.code = StatusCodes.FAILED
        }
    }  
    
    return ( status, l )   
  }
    
  
  ////////////// BLOBS ////////////////////

  // works.
  def getBlobProperties(   container:String ,blobName:String): ( Status, Blob ) = 
  {
    
    var status = new Status()
    var blob:Blob= null
    
    try
    {
      var res = blobDao.getBlobProperties( accountName, key, container, blobName )
      
      status = res._1
      blob = res._2
     
      log.debug("status code is " + status.code.toString() ) 
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::getBlobProperties exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return ( status, blob )
  }

  def setBlobProperties( container:String ,blob:Blob): Status = 
  {
    
    var status = new Status()

    try
    {
      status = blobDao.setBlobProperties( accountName, key, container, blob )
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setBlobProperties exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return status
  }
    
  def setBlobMetadata(  container:String ,  blob:Blob): Status = 
  {
    
    var status = new Status()

    try
    {
      status = blobDao.setBlobMetadata( accountName, key, container, blob )
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setBlobMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return status
  }  
  
  def setBlobMetadata(  container:String, blobName:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var header = new HashMap[String, String]()
      header( metaName ) = metaValue
    
      var blob = new Blob( blobName )
      blob.metaData = header
      
      status = blobDao.setBlobMetadata( accountName, key, container, blob )

    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setBlobMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return status
  }  

  def setBlobMetadata( container:String,  blobName:String,  keyValuePairs: HashMap[ String, String ] ): Status = 
  {
    
    var status = new Status()

    try
    {
      var blob = new Blob( blobName )
      blob.metaData = keyValuePairs
      
      status = blobDao.setBlobMetadata( accountName, key, container, blob )
 
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::setContainerMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }        
    
    return status
  }  
  
  def addBlobMetadata(  blobName: String, container:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var resp = blobDao.getBlobMetadata( accountName, key, container, blobName )
    
      var blob = resp._2
    
      blob.metaData( metaName) = metaValue
      
      blobDao.setBlobMetadata( accountName, key, container, blob )
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::addBlobMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }        
    return status
  } 
  

  // works.
  def getBlobMetadata(   container:String ,blobName:String): ( Status, Blob ) = 
  {
    
    var status = new Status()
    var blob:Blob= null
    
    try
    {
      var res = blobDao.getBlobMetadata( accountName, key, container, blobName )
      
      status = res._1
      blob = res._2
     
      log.debug("status code is " + status.code.toString() ) 
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::getBlobMetadata exception")
          status.code = StatusCodes.FAILED
        }
    }    
    
    return ( status, blob )
  }
    
  def listBlobs( containerName:String ): ( Status, List[Blob] ) = 
  {
    var status = new Status()
    var l = List[Blob]()
    
    try
    {
      var res  = blobDao.listBlobs( accountName, key, containerName )
      
      status = res._1
      l = res._2
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureBlobStorage::listBlobs exception " + ex.toString() )
          status.code = StatusCodes.FAILED
        }
    }
    
    return (status, l )
  }
}
