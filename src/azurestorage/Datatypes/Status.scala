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



// internal status codes. These are the ones that are returned by Azure....
// Should we pass these onto the end consumer?
object StatusCodes 
{
  val SUCCESS = 0
  val FAILED = 1
  
  // containers
  val CREATE_CONTAINER_SUCCESS = 201
  val DELETE_CONTAINER_SUCCESS = 202
  val SET_CONTAINER_METADATA_SUCCESS = 200
  val GET_CONTAINER_METADATA_SUCCESS = 200
  val SET_CONTAINER_ACL_SUCCESS = 200
  val GET_CONTAINER_ACL_SUCCESS = 200  
  val LIST_CONTAINERS_SUCCESS = 200
  
  // blobs
  val GET_BLOB_SUCCESS = 200
  val SET_BLOB_SUCCESS = 201
  
  val GET_BLOB_LIST_SUCCESS = 200
  
  val DELETE_BLOB_SUCCESS = 200
  val DELETE_BLOB_ACCEPTED_SUCCESS = 202
  
  val GET_BLOB_PROPERTIES_SUCCESS = 200
  val GET_BLOB_METADATA_SUCCESS = 200
  val SET_BLOB_METADATA_SUCCESS = 200
  
}



// code should be external status code.... ie what azure gives us. 
// BUT... we assign successful as true/false depending on what actually happened.
// reason being that a success code might be different for various operations. 
// ie for one action 200 might be success but for another 201 might be success.
// This way the client only needs to check the boolean to see if everything is ok,
// if its NOT successful then they can jump into the details of code/messages etc.
class Status 
{
  var message:String = null
  var code:Int = StatusCodes.SUCCESS
  
  var successful:boolean = false
}
