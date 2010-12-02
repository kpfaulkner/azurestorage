/* Copyright Ken Faulkner 2009. */

package azurestorage

import scala.collection.mutable.ListBuffer


import scala.collection.mutable._
import azurestorage._
import azurestorage.Datatypes._
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.io.Source
import java.util.Date

object basicblobtest
{
  Configgy.configure("azurestorage.cfg")

  var key = Configgy.config.getString("key", null)
  var acct = Configgy.config.getString("account", null)
  
  val log = Logger.get

  var as = new AzureBlobStorage( acct, key )  
  
  var testContainerName = "testcontainer"
  var testContainerName2 = "testcontainer2"
  
  
  def setBlob() =
  {
  
      var data = "dummydata".getBytes()
      
      var b = new Blob("bar", data)
      
      // create container?
      // yes contaminates results (if it fails) still, required.
      as.createContainer(  testContainerName )

      
      var status = as.setBlob( testContainerName, b)    
      
      if (status.successful)
      {
        println("setBlob successful")
      }
      else
      {
        println("setBlob NOT successful")
      }
      
      
  }
  
  def getBlob() =
  {
    // assumption that setBlob has been run first :)
    var res = as.getBlob( testContainerName, "bar")
    
    var status = res._1
    
    if (status.successful)
    {
      var blob = res._2
      println("getBlob successful " + new String( blob.data ) )
      
    }
    else
    {
      println("getBlob NOT successful")
    }    
  }

  def getBlobMetadata() =
  {
    // assumption that setBlob has been run first :)
    var res = as.getBlobMetadata( testContainerName, "bar")
    
    var status = res._1
    
    if (status.successful)
    {
      var blob = res._2
      println("getBlobMetadata successful " +blob.metaData.toString() )
      
    }
    else
    {
      println("getBlobMetadata NOT successful")
    }    
  }  

  def getBlobProperties() =
  {
    // assumption that setBlob has been run first :)
    var res = as.getBlobProperties( testContainerName, "bar")
    
    var status = res._1
    
    if (status.successful)
    {
      var blob = res._2
      println("getBlobProperties successful " +blob.metaData.toString() )
      
    }
    else
    {
      println("getBlobProperties NOT successful")
    }    
  }
  def setBlobMetadata() =
  {
    // assumption that setBlob has been run first :)
    
    var blob = new Blob("bar")
    var hm = new HashMap[String, String]()
    hm("aaaa") = "bar"
    hm("bbbb") = "kjk"
    
    blob.metaData = hm
    
    var status = as.setBlobMetadata( testContainerName, blob )

    
    if (status.successful)
    {

      println("setBlobMetadata successful ")
      
    }
    else
    {
      println("setBlobMetadata NOT successful")
    }    
  }

  def setBlobProperties() =
  {
    // assumption that setBlob has been run first :)
    
    var blob = new Blob("bar")
    var hm = new HashMap[String, String]()
    hm("foo") = "bar"
    hm("kenny") = "kjk"
    
    blob.metaData = hm
    
    var status = as.setBlobProperties( testContainerName, blob)

    
    if (status.successful)
    {

      println("setBlobProperties successful ")
      
    }
    else
    {
      println("setBlobProperties NOT successful")
    }    
  }
      
  def createContainer() =
  {
    var status = as.createContainer(  testContainerName2 )
    
    if (status.successful)
    {
      println("createContainer successful")
    }
    else
    {
      println("createContainer NOT successful")
    }      
  }

  def listContainers() =
  {
    var res = as.listContainers()
    var status = res._1
    
    if (status.successful)
    {
      println("listContainers successful")
    }
    else
    {
      println("listContainers NOT successful")
    }      
  }

  def setContainerMetadata() =
  {

    var status = as.setContainerMetadata( testContainerName, "foo", "bar" )
    
    if (status.successful)
    {
      println("setContainerMetadata successful")
    }
    else
    {
      println("setContainerMetadata NOT successful")
    }      
  }  
  
  def getContainerMetadata() =
  {

    var res = as.getContainerMetadata( testContainerName )
    
    var status = res._1
    
    if (status.successful)
    {
      println("getContainerMetadata successful")
    }
    else
    {
      println("getContainerMetadata NOT successful")
    }      
  }    
  
  def deleteContainer() =
  {
    var status = as.deleteContainer(  testContainerName2 )
    
    if (status.successful)
    {
      println("deleteContainer successful")
    }
    else
    {
      println("deleteContainer NOT successful")
    }      
  }
  
  def main(args: Array[String]) =
  {

    //deleteContainer()
    //setBlob()
    
    
    //getBlob()
    setBlobProperties()
    getBlobProperties()
    setBlobMetadata()
    getBlobMetadata()
    
    /*
    createContainer()
    listContainers()
    setContainerMetadata()
    getContainerMetadata()
    
    
    deleteContainer()
    */
    
    /*
    var l = List[ContainerACL]()
    
    var acl = new ContainerACL()
    acl.uid = "FFF"
    acl.startTime = new Date()
    acl.endTime = new Date(110,0,1)
    acl.canRead = true
    acl.canWrite = false
    acl.canDelete = true
    
    l += acl
    
    as.setContainerACL( "foo2", l, true  )
    var resp2 = as.getContainerACL( "foo2"  )
    
    println( "acls are " + resp2._2.toString() )
    println( "acl " + resp2._2(0).uid.toString() )
    println( "acl " + resp2._2(0).startTime.toString() )
    println( "acl " + resp2._2(0).endTime.toString() )
    
    */
    //as.createContainer(  args(0) )
    
    //as.setContainerMetadata( args(0), "foo", "bar") 

    //var resp = as.getContainerMetadata( args(0) ) 

    //println("headers " + resp._2.toString() )

    //var resp2 = as.deleteContainer( args(0) ) 

    //println("xxxx " + resp2.toString() )


    //as.setBlobByFilename( "fodddo3", "/tmp/ken.txt", "foo")

    
    /*
    var b = new Blob("bar")
    b.data = new Array[Byte]( 3 )
    b.data(0) = 60
    b.data(1) = 61
    b.data(2) = 61
    
    
    as.setBlob( "foo2", b)
    
    
    var bb = as.getBlob( "foo2", "bar")
    

    println("data is " + bb._2.data.toString() )
    
    var resp3 = as.listContainers()
    println("resp " + resp3.toString() )
    
    */
  }
}


