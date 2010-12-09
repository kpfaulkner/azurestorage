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


class AzureContext( a:String, k:String)
{
  val key = k
  val accountName = a
}



// main object for blob access.
object AzureBlobClient
{
  
  val blobDao = new AzureStorageBlobDAO()
  
  Configgy.configure("azurestorage.cfg")
  val log = Logger.get
  
  
  
  // filename is fully qualified.
  // destination is the location within the container.
  def setBlobByFilename( context:AzureContext, containerName: String, filename: String, destination:String ): Status =
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
        
      status = blobDao.putBlob( context.accountName, context.key, containerName, blob )

    }
    catch
    {
      // nasty general catch...
      // but given this is the highest level...  I'll live with it for now.
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
  def putBlock( context:AzureContext, containerName: String, block: Block ): Status =
  {
    var status = new Status()

    return status
  }

  // put block list.
  def putBlockList( context:AzureContext, containerName: String, blockList: Array[ String ], coverBlob:Blob ): Status =
  {
        var status = new Status()

    return status
  }
  
  def getBlockList( context:AzureContext, containerName: String, blobName: String ): ( Status, Array[ String] ) =
  {
        var status = new Status()

    return ( status, null )
  }
    
  def putBlob( context:AzureContext, containerName: String, blob: Blob ): Status =
  {
    var status = new Status()
    
    try
    {
      status = blobDao.putBlob( context.accountName, context.key, containerName, blob )

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

  def deleteBlob( context:AzureContext, containerName: String, blobName: String ):  Status  =
  {
  
    
    var status = new Status()
    
    try
    {
      status = blobDao.deleteBlob( context.accountName, context.key, containerName, blobName )
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
    
  def getBlob( context:AzureContext, containerName: String, blobName: String ): ( Status, Blob ) =
  {
  
    var blob:Blob = null
    
    var status = new Status()
    
    try
    {
      var res = blobDao.getBlob( context.accountName, context.key, containerName, blobName )
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
  def setContainerACL( context:AzureContext, containerName: String, ACLList:List[ ContainerACL ], isPublic:boolean ): Status =
  {
    var status = new Status()
    
    try
    {
      status = containerDao.setContainerACL( context.accountName, context.key, containerName, ACLList, isPublic )

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
  
  def getContainerACL( context:AzureContext, containerName:String ): (Status, List[ ContainerACL ] ) = 
  {
    var status = new Status()
    
    var l:List[ ContainerACL] = null
    
    try
    {
      var res = containerDao.getContainerACL( context.accountName, context.key, containerName )

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
  def getBlobProperties(  context:AzureContext, containerName:String ,blobName:String): ( Status, Blob ) = 
  {
    
    var status = new Status()
    var blob:Blob= null
    
    try
    {
      var res = blobDao.getBlobProperties( context.accountName, context.key, containerName, blobName )
      
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

  def setBlobProperties( context:AzureContext, containerName:String ,blob:Blob): Status = 
  {
    
    var status = new Status()

    try
    {
      status = blobDao.setBlobProperties( context.accountName, context.key, containerName, blob )
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
    
  def setBlobMetadata(  context:AzureContext, containerName:String ,  blob:Blob): Status = 
  {
    
    var status = new Status()

    try
    {
      status = blobDao.setBlobMetadata( context.accountName, context.key, containerName, blob )
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
  
  def setBlobMetadata(  context:AzureContext, containerName:String, blobName:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var header = new HashMap[String, String]()
      header( metaName ) = metaValue
    
      var blob = new Blob( blobName )
      blob.metaData = header
      
      status = blobDao.setBlobMetadata( context.accountName, context.key, containerName, blob )

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

  def setBlobMetadata( context:AzureContext, containerName:String,  blobName:String,  keyValuePairs: HashMap[ String, String ] ): Status = 
  {
    
    var status = new Status()

    try
    {
      var blob = new Blob( blobName )
      blob.metaData = keyValuePairs
      
      status = blobDao.setBlobMetadata( context.accountName, context.key, containerName, blob )
 
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
  
  def addBlobMetadata( context:AzureContext, blobName: String, containerName:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var resp = blobDao.getBlobMetadata( context.accountName, context.key, containerName, blobName )
    
      var blob = resp._2
    
      blob.metaData( metaName) = metaValue
      
      blobDao.setBlobMetadata( context.accountName, context.key, containerName, blob )
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
  def getBlobMetadata(  context:AzureContext, containerName:String ,blobName:String): ( Status, Blob ) = 
  {
    
    var status = new Status()
    var blob:Blob= null
    
    try
    {
      var res = blobDao.getBlobMetadata( context.accountName, context.key, containerName, blobName )
      
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
    
  def listBlobs( context:AzureContext, containerName:String ): ( Status, List[Blob] ) = 
  {
    var status = new Status()
    var l = List[Blob]()
    
    try
    {
      var res  = blobDao.listBlobs( context.accountName, context.key, containerName )
      
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
