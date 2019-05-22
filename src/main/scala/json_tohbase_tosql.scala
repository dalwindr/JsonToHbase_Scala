//sscala -classpath target/scala-2.11/json_tohbase_tosql_2.11-0.1.jar json_tohbase_tosql
//spark-submit --class json_tohbase_tosql --driver-class-path=/usr/local/Cellar/apache-spark/2.4.0/libexec/jars/*:/usr/local/Cellar/external-jars/*:/usr/local/Cellar/hbase-1.4.9/lib/* target/scala-2.11/json_tohbase_tosql_2.11-0.1.jar 
//java -jar target/scala-2.11/json_tohbase_tosql_2.11-0.1.jar json_tohbase_tosql
import scala.io._
import java.io.{FileNotFoundException, IOException}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

import java.util.Date
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.parseJson
import org.json4s.JsonDSL._

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor,HColumnDescriptor,HConstants,TableName,CellUtil}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result,Put,HTable,ConnectionFactory,Connection,Get,Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import sys.process._

case class _iTemIdL3(kind: String,channelId: Option[String],videoId: Option[String])
case class _iTems(kind: String,etag: String, id: _iTemIdL3)
case class _PageInfo(totalResults: Int,resultsPerPage: Int)
case class _youtubeJson(kind: String, etag: String,nextPageToken: String,regionCode: String, pageInfo: _PageInfo,items: List[_iTems])
case class _loadingJson(rowKey: String , kind: String, etag: String, nextPageToken: String, regionCode: String, totalResults: Int, 
						resultsPerPage: Int, items_kind: String, items_etag: String, items_kind_ofKind: String, 
						item_channelId: Option[String], item_videoId: Option[String])

object json_tohbase_tosql {

		//Connection to Hbase
		def ConnectToHbase() : Connection = {

		      val hconf = HBaseConfiguration.create()
		      hconf.set("hbase.zookee per.quorum","localhost")
		      hconf.set("hbase.zookeeper.property.clientPort","2181") 
		      val hconnection=ConnectionFactory.createConnection(hconf)
		      hconnection
		    }  

		//Parse Json
		def parseJsonFile(fileName: String) = {
		    implicit val formats = org.json4s.DefaultFormats
		    
		    var jsonToString = scala.io.Source.fromFile(fileName).getLines.mkString   //.toArry // .toList
		    var jsonData= parseJson(jsonToString)
		    print(pretty(render(jsonData)))
		    var parsed_data= jsonData.extract[_youtubeJson]
		    parsed_data
		    // (_row_key_, parsed_data.kind, parsed_data.etag, parsed_data.nextPageToken, parsed_data.regionCode,totalResults, resultsPerPage,
		    //	    _Data_kind_,  _Data_etag_, _Data_Kind_Ofkind_, _Data_Kind_OfchannelId_,  _Data_Kind_OfvideoId_ )
		}

		//Load Json
		def loadJSON_toHbase(Data: _loadingJson, connection: org.apache.hadoop.hbase.client.Connection ) = {

				val open_table = connection.getTable(TableName.valueOf("youtube:youtube_searches"))
				var rk = Data.rowKey
		        var p = new Put(new String(rk).getBytes()) 
		       
		        p.add("criterias".getBytes(),"kind".getBytes(),new String(Data.kind).getBytes())
		        p.add("criterias".getBytes(),"etag".getBytes(),new String(Data.etag).getBytes())
		        p.add("criterias".getBytes(),"nextPageToken".getBytes(),new String(Data.nextPageToken).getBytes())
		        p.add("criterias".getBytes(),"regionCode".getBytes(),new String(Data.regionCode).getBytes())

		        p.add("pageinfo".getBytes(),"totalResults".getBytes(),Array(new Integer(Data.totalResults).toByte)) //.getBytes())
		        p.add("pageinfo".getBytes(),"resultsPerPage".getBytes(),Array(new Integer(Data.resultsPerPage).toByte))//.getBytes())

		        p.add("items".getBytes(),"kind".getBytes(),new String(Data.items_kind).getBytes())
		        p.add("items".getBytes(),"etag".getBytes(),new String(Data.items_etag).getBytes())

		        p.add("subitem".getBytes(),"kind".getBytes(),new String(Data.items_kind_ofKind).getBytes())
		     //    if (!Option(Data.item_channelId).getOrElse("").isEmpty())
		     //    { 
		     //     p.add("itemSubkind".getBytes(),"channelId".getBytes(),new String((Data.item_channelId).mkString).getBytes())
		    	//  }
		    	// if (!(Option(Data.item_videoId)).isEmpty())
		     //    { 
		     //     p.add("itemSubkind".getBytes(),"videoId".getBytes(),new String((Data.item_videoId).mkString).getBytes())
		     //     }
		        open_table.put(p)
		     
		     //table.flushCommits()
		     open_table.close()
		     //connection.close()
		}

		//Main Function
		def main(args: Array[String]) {
		    println("test program ..........................")

		   
		   var jsonFile = "/Users/keeratjohar2305/Downloads/youTube_sampleJson"
		   var jsonFile = "/Users/keeratjohar2305/Downloads/Json_project/Data_location/youTube_sampleJson.json"
		   var parsed_JsonData = parseJsonFile(jsonFile)
		  
		    var totalResults = parsed_JsonData.pageInfo.totalResults
		    var resultsPerPage = parsed_JsonData.pageInfo.resultsPerPage
		    var item_no=0
		    val connection = ConnectToHbase()
		    for ( _Data<- parsed_JsonData.items) {
		    	
		    	item_no = item_no + 1
		    	var _Data_kind_ = _Data.kind
		    	var _Data_etag_ = _Data.etag
		    	var _Data_Kind_Ofkind_ =_Data.id.kind
		    	var _Data_Kind_OfchannelId_ =_Data.id.channelId.mkString
		    	var _Data_Kind_OfvideoId_ =_Data.id.videoId.mkString
		    	var _row_key_ = parsed_JsonData.kind + "_" + item_no.toString
		    	println(_row_key_, parsed_JsonData.kind, parsed_JsonData.etag, parsed_JsonData.nextPageToken, parsed_JsonData.regionCode,
		        	       totalResults, resultsPerPage, _Data_kind_,  _Data_etag_, _Data_Kind_Ofkind_, _Data_Kind_OfchannelId_, 
		        	        _Data_Kind_OfvideoId_ )
		     
		       var parsed_data= _loadingJson(_row_key_, parsed_JsonData.kind, parsed_JsonData.etag, parsed_JsonData.nextPageToken, parsed_JsonData.regionCode,
		        	       totalResults, resultsPerPage, _Data_kind_,  _Data_etag_, _Data_Kind_Ofkind_, Option(_Data_Kind_OfchannelId_),  Option(_Data_Kind_OfvideoId_) )
		        loadJSON_toHbase(parsed_data,connection)
             }
             connection.close()
         }
		// create_namespace 'youtube'
		// list_namespace
		// create 'youtube:youtube_searches', 'criterias', 'pageinfo','items', 'itemss
		// list_namespace_tables 'youtube'
		// scan 'youtube:youtube_searches' {LIMIT=> 10}
		// get 'nysc:test',1


    
}

//import org.json4s.Formats
//import org.json4s.Formats._
//import org.json4s.DefaultFormats
//import org.json4s.DefaultFormats._
//import java.sql.{Connection, DriverManager, ResultSet};

    //val url = "jdbc:mysql://localhost:3306/testabc"
    //val driver = "com.mysql.jdbc.Driver"
    //val username = "testabc"
    //val password = "testabc"
    //var connection:Connection = _
    
	 // try  {
		// Class.forName(driver)
  //       var connection = DriverManager.getConnection(url, username, password)     	
      
  //       val statement = connection.createStatement
  //       val rs = statement.executeQuery("select  id, name from example")
  //       while (rs.next) {
  //           val host = rs.getString("id")
  //           val user = rs.getString("name")
  //           println("host = %s, user = %s".format(host,user))
  //           }
  //       connection.close() 
        
  //       } catch { case e: Exception => e.printStackTrace }

    //implicit val formats1 = org.json4s.Formats
