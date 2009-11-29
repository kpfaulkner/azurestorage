/* Copyright Ken Faulkner 2009. */

package azurestorage

import scala.collection.mutable.ListBuffer


import scala.collection.mutable.Map
import azurestorage._
import azurestorage.Datatypes._
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import scala.io.Source
import java.util.Date

object servertest
{
   def main(args: Array[String]) =
    {
      Configgy.configure("azurestorage.cfg")

      println("first arg " + args(0) )
      
      var key = Configgy.config.getString("key", null)
      var acct = Configgy.config.getString("account", null)
      
      val log = Logger.get

      //var as = new AzureBlobStorage("kenstore", "UKJ3i0zABCA2K313GrjI8AF/RJpUzl4TuGD8cHdv4wlnvTFgh8Vhg23YCf4cPH3ojCfFSMvKit2m6XKgrYxCKw==")
      var as = new AzureBlobStorage( acct, key )
      
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
      var bb = as.getBlob( "private", "foo/bar/k.txt")
      

      println("data is " + bb._2.data.toString() )
      
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


