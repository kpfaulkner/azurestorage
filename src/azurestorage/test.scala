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

object bAzureBlobClient.cblobtest
{
  Configgy.configure("azurestorage.cfg")

  var key = Configgy.config.getString("key", null)
  var acct = Configgy.config.getString("account", null)
  
  val log = Logger.get

  var context = new AzureContext( acct, key )  
  
  var testContainerName = "testcontainer"
  var testContainerName2 = "testcontainer2"
  
  
  def putBlob( name:String ) =
  {
  
      var data = "dummydata".getBytes()
      
      var b = new Blob( context, name , data)
      
      // create container?
      // yes contaminates results (if it fails) still, required.
      AzureBlobClient.createContainer(  testContainerName )

      
      var status = AzureBlobClient.putBlob( context, testContainerName, b)    
      
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
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    var res = AzureBlobClient.getBlob( context, testContainerName, "bar")
    
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
  def listBlobs() =
  {
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    var res = AzureBlobClient.listBlobs( context, testContainerName )
    
    var status = res._1
    
    if (status.successful)
    {
      println("listBlobs successful " )
      
      var blobs = res._2
      for ( b <- blobs )
      {
        println("blob " + b.toString() )
      }
    }
    else
    {
      println("listBlobs NOT successful")
    }    
  }

  def deleteBlob() =
  {
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    var status = AzureBlobClient.deleteBlob(context,  testContainerName, "bar")
    

    
    if (status.successful)
    {
    
      println("deleteBlob successful "  )
      
    }
    else
    {
      println("deleteBlob NOT successful")
    }    
  }

  def getBlobMetadata() =
  {
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    var res = AzureBlobClient.getBlobMetadata(context,  testContainerName, "bar")
    
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
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    var res = AzureBlobClient.getBlobProperties( context, testContainerName, "bar")
    
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
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    
    var blob = new Blob("bar")
    var hm = new HAzureBlobClient.Map[String, String]()
    hm("aaaa") = "bar"
    hm("bbbb") = "kjk"
    
    blob.metaData = hm
    
    var status = AzureBlobClient.setBlobMetadata( context, testContainerName, blob )

    
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
    // AzureBlobClient.umption that setBlob hAzureBlobClient.been run first :)
    
    var blob = new Blob("bar")
    var hm = new HAzureBlobClient.Map[String, String]()
    hm("foo") = "bar"
    hm("kenny") = "kjk"
    
    blob.metaData = hm
    
    var status = AzureBlobClient.setBlobProperties(context,  testContainerName, blob)

    
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
    var status = AzureContainerClient.createContainer( context,  testContainerName2 )
    
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
    var res = AzureContainerClient.listContainers(context )
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

    var status = AzureContainerClient.setContainerMetadata( context, testContainerName, "foo", "bar" )
    
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

    var res = AzureContainerClient.getContainerMetadata(context,  testContainerName )
    
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
    var status = AzureContainerClient.deleteContainer( context,  testContainerName2 )
    
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
    putBlob("bar")
    putBlob("foo")
    putBlob("abc")
    
    
    
    //getBlob()
    //setBlobProperties()
    setBlobMetadata()
    getBlobMetadata()
    //getBlobProperties()
    listBlobs()

    //deleteBlob()

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
    
    AzureBlobClient.setContainerACL( "foo2", l, true  )
    var resp2 = AzureBlobClient.getContainerACL( "foo2"  )
    
    println( "acls are " + resp2._2.toString() )
    println( "acl " + resp2._2(0).uid.toString() )
    println( "acl " + resp2._2(0).startTime.toString() )
    println( "acl " + resp2._2(0).endTime.toString() )
    
    */
    //AzureBlobClient.createContainer(  args(0) )
    
    //AzureBlobClient.setContainerMetadata( args(0), "foo", "bar") 

    //var resp = AzureBlobClient.getContainerMetadata( args(0) ) 

    //println("headers " + resp._2.toString() )

    //var resp2 = AzureBlobClient.deleteContainer( args(0) ) 

    //println("xxxx " + resp2.toString() )


    //AzureBlobClient.setBlobByFilename( "fodddo3", "/tmp/ken.txt", "foo")

    
    /*
    var b = new Blob("bar")
    b.data = new Array[Byte]( 3 )
    b.data(0) = 60
    b.data(1) = 61
    b.data(2) = 61
    
    
    AzureBlobClient.setBlob( "foo2", b)
    
    
    var bb = AzureBlobClient.getBlob( "foo2", "bar")
    

    println("data is " + bb._2.data.toString() )
    
    var resp3 = AzureBlobClient.listContainers()
    println("resp " + resp3.toString() )
    
    */
  }
}


