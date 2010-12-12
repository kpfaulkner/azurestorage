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


// main object for blob access.
object AzureContainerClient
{
  
  val containerDao = new AzureStorageContainerDAO()
  
  Configgy.configure("azurestorage.cfg")
  val log = Logger.get
  
  def createContainer( context:AzureContext, containerName:String ): Status = 
  {
    var status = new Status()
  
    try
    {
      status = containerDao.createContainer( context.accountName, context.key, containerName )
    
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

  def listContainers( context:AzureContext ): ( Status, List[Container] ) = 
  {
    var status = new Status()
    var l = List[Container]()
    
    try
    {
      var res  = containerDao.listContainers( context.accountName, context.key )
      
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
    
  def deleteContainer( context:AzureContext, containerName:String ): Status = 
  {
    
    var status = new Status()
    
    try
    {
      containerDao.deleteContainer( context.accountName, context.key, containerName )
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
  
  def setContainerMetadata( context:AzureContext, containerName:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var header = Map[String, String]()

      // not sure I like the header instance being created at this level.
      header( metaName ) = metaValue
    
      containerDao.setContainerMetadata( context.accountName, context.key, containerName, header )

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


  // set container ACL via container string name.
  def setContainerACL( context:AzureContext, containerName: String, ACLList:List[ ContainerACL ], isPublic:Boolean ): Status =
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

  def setContainerMetadata(  context:AzureContext, containerName:String,  keyValuePairs: Map[ String, String ] ): Status = 
  {
    
    var status = new Status()

    try
    {
      containerDao.setContainerMetadata( context.accountName, context.key, containerName, keyValuePairs )
 
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
  
  def addContainerMetadata(  context:AzureContext, containerName:String,  metaName:String, metaValue:String  ): Status = 
  {
    
    var status = new Status()

    try
    {
      var resp = containerDao.getContainerMetadata( context.accountName, context.key, containerName )
    
      var headers = resp._2
    
      // not sure I like creating headers at this level. FIXME
      headers( metaName ) = metaValue
    
      containerDao.setContainerMetadata( context.accountName, context.key, containerName, headers )
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
    
  def getContainerMetadata(  context:AzureContext, containerName:String ): ( Status, Map[String, String]  ) = 
  {
    
    var status = new Status()

    var metadata:Map[String, String] = null
    
    try
    {
      var resp = containerDao.getContainerMetadata( context.accountName, context.key, containerName )
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
}
