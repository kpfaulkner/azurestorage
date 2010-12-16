/* Copyright Ken Faulkner 2009. */

package azurestorage.utils

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import azurestorage._
import azurestorage.Datatypes._
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.io.Source
import java.util.Date

import java.io.FileOutputStream
import java.io.FileInputStream
import java.io.DataInputStream
import java.io.DataOutputStream

// basic operations on azure storage acct.
// create container
// upload file
// download file
// set container ACL's (just public/private atm)
// delete container etc etc.
//
//
// Important note:
//   Considering you cannot have containers inside of containers, the default container will be read from the config.
//   all blobs will go into that container (private container)
object AzureCommand
{
  
  Configgy.configure("azurestorage.cfg")

  var key = Configgy.config.getString("key", null)
  var acct = Configgy.config.getString("account", null)
  
  val context = new AzureContext( acct,key)

  val log = Logger.get
    
  val defaultPrivateContainer = Configgy.config.getString("default_private_container", "private")
  val defaultPublicContainer = Configgy.config.getString("default_public_container", "public")
  
  
  // parameters...  yeah yeah global, but this is an object and very short life span.
  var filename = ""
  var container = ""
  var destination = ""
  
  val azurePrefix = "azr:"
  


  // put blob.
  // assume origin is correct path to file. eg /tmp/foo.txt
  // destination is azr://container
  private def doPut( origin: String, destination:String ) =
  {
    // FIXME: need to be OS agnostic.
    var sp = destination.split("/")
    
    var container = sp.last
    
    sp = origin.split("/")
    var baseFile = sp.last
    
    var status = AzureBlobClient.putBlobByFilePath( context, container, origin , baseFile )
    if (status.code != StatusCodes.SET_BLOB_SUCCESS )
    {
      println("Failed to copy")
    }
    else
    {
      println("Copied successfully")
    }
  }
  
  // write data contents of blob as file.
  private def writeFile( destination: String, blob:Blob ) =
  {
    var fn = destination + blob.name
    
    var outFileStream = new FileOutputStream( fn, false )
    var dataOutputStream = new DataOutputStream( outFileStream )
  
    dataOutputStream.write( blob.data )
     
    dataOutputStream.close()
    
  }
  
  // get blob.
  // assume origin path to azure blob. eg, azr://container/myfile.txt
  // destination is directory to write too. eg /tmp/   
  // 
  // No real error checking.
  private def doGet( origin: String, destination:String ) =
  {

    // make sure destination ends in /
    var dest = destination
    if ( dest.last != '/')
    {
      dest += "/"
    }    
    
    var sp = origin.split("/")
    var baseFile = sp.last
    var container = sp(2)
    
    var res = AzureBlobClient.getBlob( context, container, baseFile )
    
    var status = res._1
    var blobOption = res._2
    
    
    if ( status.successful )
    {
      if ( blobOption != None )
      {
        println("Success")
        var blob = blobOption.get
        
        // display the metadata.
        for ( k <- blob.metaData.keys )
        {
          println( k + " : " + blob.metaData(k) )
        }
        writeFile( dest, blob )
      }
      else
      {
        println("failed 1")
      }
      
    }
    else
    {
      println("failed 2")
    }
    
  }
  
  
  
  
  
  // 1) localfile  destination.
  //    /tmp/foo.txt /container/filename.txt
  // 
  //
  def doCopy( args: Array[String ]) =
  {
    log.info("AzureCommand::doCopy start")
    
    try
    {
   
      var l = args.toList
      
      var origin = args(0)
      var destination = args(1)
      
      if ( origin.startsWith( azurePrefix ) )
      {
        doGet( origin, destination )
      }
      else if ( destination.startsWith( azurePrefix ))
      {
        doPut( origin, destination )
      }
      
          
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureCommand::doCopy exception " + ex.toString() )
          //status.code = StatusCodes.FAILED
          //status.message = "Failed to set blob by filepath"
        }
    }  
    
  }

  def doMakeContainer( args: Array[String ]) =
  {
    log.info("AzureCommand::doMakeContainer start")
    
    try
    {
   
      // just accept container name.... thats it.
      var container = args( 0 )
     
      
      var status = AzureContainerClient.createContainer( context, container )
     
      if (status.code != StatusCodes.SUCCESS )
      {
        println("Failed to create container")
      }
      
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureCommand::doMakeContainer exception " + ex.toString() )
          //status.code = StatusCodes.FAILED
          //status.message = "Failed to set blob by filepath"
        }
    }      
  }
    
  def doMove( args: Array[String ]) =
  {
    
  }
  
  def doRemove( args: Array[String ]) =
  {
    
  }
  
  def doChmod( args: Array[String ]) =
  {
    
  }
  
  def doOops( args: Array[String ]) =
  {
    println("Sorry, command " + args(0) + " does not exist" ) 
  }
  
  
  // rules.
  // args(0) is the command. cp, rm, mkdir etc....
  // args(1) 
  def parse( args: Array[String]) =
  {
     var methodArgs = args.drop(1)
     args(0) match
     {
       case "cp" => doCopy( methodArgs )
       case "mv" => doMove( methodArgs )
       case "rm" => doRemove( methodArgs )
       case "chmod" => doChmod( methodArgs)
       case "mkdir" => doMakeContainer( methodArgs )
       
       case other => doOops( methodArgs )
       
     }
  }
   
  def main(args: Array[String]) =
  {


      parse( args )
      
    }
}


