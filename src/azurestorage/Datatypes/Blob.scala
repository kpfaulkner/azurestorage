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

package azurestorage.Datatypes

import scala.collection.mutable._

import scala.collection.mutable.Map


// probably should just use an enumerator.
object BlobType
{
  val BLOCKBLOB = "BlockBlob"
  val PAGEBLOB = "PageBlob"
}

object BlobProperty
{
  var contentType="x-ms-blob-content-type"
  var contentEncoding = "x-ms-blob-content-encoding"
  var contentLanguage = "x-ms-blob-content-language"
  var cacheControl = "x-ms-blob-cache-control"
  var contentMD5 = "x-ms-blob-content-md5"
  var blobType = "x-ms-blob-type"
  var metaName = "x-ms-meta-"  // this is a prefix to a custom property name. eg, x-ms-meta-foo = 123
  var leaseId = "x-ms-lease-id"
  
}


class Blob( blobName:String )
{

  var name:String = blobName
  
  // combined properties and metadata for blob.
  // should I split them out?
  var metaData = new HashMap[String, String]()

  setupDefaultProperties( )
  
  var data:Array[Byte] = null
      
  // constructor with blob data.
  // once convert to scala 2.8 will just settle for default params.
  def this( blobName: String, blobData:Array[Byte] ) =
  {
    this( blobName )
    data = blobData
  }
  
  // only setup blobType by default.
  def setupDefaultProperties( ) =
  {
    metaData( BlobProperty.blobType ) = BlobType.BLOCKBLOB
  }

  // debugging crap
  override def toString(): String =
  {
    var s = "name: " + name
    s += "\nmetadata: " + metaData.toString() 
    
    return s  
  }
}
