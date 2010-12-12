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


// main object for queue access.
object AzureQueueClient
{
  
  val queueDao = new AzureStorageQueueDAO()
  
  Configgy.configure("azurestorage.cfg")
  val log = Logger.get
  
  
  // was going to list AzureQueue instances, but really thinking Strings might be the way
  // to go atm.
  def listQueues( context:AzureContext ): ( Status, List[ String ] ) =
  {
    var status = new Status()
    
    return (status, null )
  }
  
  def createQueue( context:AzureContext, queueName:String ) : Status =
  {
    var status = new Status()
    
    return status
  }
  
  def deleteQueue( context:AzureContext, queueName:String ): Status =
  {
    var status = new Status()
  
    return status
  }
  
  def getQueueMetaData( context:AzureContext, queueName:String ): (Status, HashMap[String,String]) =
  {
    var s = new Status()
    
    return (s, null)
  }
  
  def setQueueMetaData( context:AzureContext, queueName:String, metaData:HashMap[String,String] ): Status =
  {
    var s = new Status()
    
    return s
  }
  
  // should I be passing about some instance of a 'Queue' class...  and act on that?
  // For now will just try and keep it simple with queuename etc...
  // That is inconsistent to the Blob storage part... but will see what I can do.
  // message... should it be string or byte array?
  def putMessage( context:AzureContext, queueName:String, message:String): Status =
  {
    var status = new Status()
    
    return status
  }

  def getMessage( context:AzureContext, queueName:String) : (Status, QueueMessage ) =
  {
  
    var status = new Status()
    
    return (status, null )
  }

  def peekMessage( context:AzureContext, queueName:String) : (Status, QueueMessage ) =
  {
  
    var status = new Status()
    
    return (status, null )
  }
  
  
  def deleteMessage( context:AzureContext, queueName:String, popReceipt:String  ): Status =
  {
  
    var status = new Status()
    
    return status
    
  }

  def clearMessages( context:AzureContext, queueName:String ) :Status =
  {
  
    var status = new Status()
    
    return status
  }
  
}
