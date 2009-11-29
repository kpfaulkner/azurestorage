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
object AzureLoader
{
  
  Configgy.configure("azurestorage.cfg")

  var key = Configgy.config.getString("key", null)
  var acct = Configgy.config.getString("account", null)
  
  val log = Logger.get
    
  val defaultPrivateContainer = Configgy.config.getString("default_private_container", "private")
  val defaultPublicContainer = Configgy.config.getString("default_public_container", "public")
  
  
  // parameters...  yeah yeah global, but this is an object and very short life span.
  var filename = ""
  var container = ""
  var destination = ""
  
  // parse the args....
  // Use matchers... and see if I screw it up? :)
  def parseCopyArgs( args: List[ String ] ): (String, String, String) = 
  {
 
    var filename = ""
    var destination = ""      
    var container = defaultPrivateContainer
    
    log.debug("args are " + args.toString() )
    args match {
      
      
      case fn::cont::dest::Nil   =>
        log.debug("1COPY " + fn + " : " + cont + " : " + dest )
        filename = fn
        destination = dest
        container = cont
        
      case fn :: dest :: Nil =>
        log.debug("2COPY " + fn + " : " + dest )
        filename = fn
        destination = dest

      case other => println("UNKNOWN")
    }
    
    return (filename, destination, container )
  }

   
  // default action is to put it in the "private" container.
  // args are command line parameters.
  // There are currently 2 options.
  // 1) localfile  destination.
  //    /tmp/foo.txt /my/azure/directory/and/filename.txt
  // 
  // 2) localfile container destination
  //    /tmp/foo.txt mycontainername /my/azure/directory/within/the/container/and/filename.txt
  //
  // We *could* make it so the container name is just the first part of the destination directory structure, but will
  // keep it separate for the moment.
  def doCopy( args: Array[String ]) =
  {
    log.info("AzureLoader::doCopy start")
    
    try
    {
   
      var l = args.toList
      var params = parseCopyArgs( l )
      
      var filename = params._1
      var destination = params._2  
      var container = params._3

           
      log.debug("copying " + filename + " to " + destination + " using container " + container )
     
      var as = new AzureBlobStorage( acct, key )
      
      var status = as.setBlobByFilename( container, filename, destination )
     
      if (status.code != StatusCodes.SET_BLOB_SUCCESS )
      {
        println("Failed to copy")
      }
      
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureLoader::doCopy exception " + ex.toString() )
          //status.code = StatusCodes.FAILED
          //status.message = "Failed to set blob by filepath"
        }
    }  
    
  }

  def doMakeContainer( args: Array[String ]) =
  {
    log.info("AzureLoader::doMakeContainer start")
    
    try
    {
   
      // just accept container name.... thats it.
      var container = args( 0 )
     
      var as = new AzureBlobStorage( acct, key )
      
      var status = as.createContainer( container )
     
      if (status.code != StatusCodes.SUCCESS )
      {
        println("Failed to create container")
      }
      
    }
    catch
    {
      // nasty general catch...
      case ex: Exception => {
          log.error("AzureLoader::doMakeContainer exception " + ex.toString() )
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


