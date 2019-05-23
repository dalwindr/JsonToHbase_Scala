//package xmltokafka.xml
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

/**
  * Created by root on 7/28/17.
  */

class YoutubeJsonGenerator(xmlTemplate:String, json_path:String) {
  private val generalPattern = """(.*\{.*|.*\.*}|.*:##.*##|.*\[.*|.*\].*)""".r
  private val timestampPattern ="""##RANDOM_KINDS##""".r
  private val intPattern =""".*##RANDOM_INT\((\d+.*\d+)\)##.*""".r
  private val stringPattern =""".*##RANDOM_STRING\((.*)\)##.*""".r
  private val doublePattern =""".*##RANDOM_DOUBLE\((\d+.*\d+)\)##.*""".r
  private val booleanPattern =""".*##RANDOM_BOOLEAN\((.*)\)##.*""".r
  val random = new scala.util.Random
  //var xmlTemplate= "/Users/keeratjohar2305/Downloads/SPARK_POC/conf/example.json"
  //private val conf = new Conf(config)
  private var template = scala.io.Source.fromFile(xmlTemplate).mkString
  private val logger = LoggerFactory.getLogger(getClass)

  def start(): Unit ={

    // val properties = new Properties()
    // comma separated list of Kafka brokers
    // properties.setProperty("bootstrap.servers", s"${conf.broker}:${conf.port}")
    // properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // properties.put("key-class-type", "java.lang.String")
    // properties.put("value-class-type", "java.lang.String")

    // val producer = new KafkaProducer[String, String](properties)
    // val date = raw"(\d{4})-(\d{2})-(\d{2})".r
    // val dates = "Important dates in history: 2004-01-20, 1958-09-05, 2010-10-06, 2011-07-15"
    // date.findAllIn(dates)
    var i=0
    val frequency=30
    var delay = 30
    while ( i != frequency)
    { 
    var jsonData =generalPattern.findAllIn(template).map{x=> 
                var data = if ( (x.split(":")).length> 1) { x.split(":")(1).toString }
                var randomNum = data match {
                case "##RANDOM_KINDS##" => RandomJsonString(data.toString)
                case "##RANDOM_ETAGS##" => RandomJsonString(data.toString)
                case "##RANDOM_NEXTPAGETOKEN##" => RandomJsonString(data.toString)
                case "##RANDOM_REGIONCODE##" => RandomJsonString(data.toString)

                case "##RANDOM_TOTALRESULT##" => RandomJsonInt(data.toString)
                case "##RANDOM_RESULTPERPAGE##" => RandomJsonInt(data.toString)

                case "##RANDOM_ITEMKIND##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMETAG##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMKIND_OFFKIND##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMCHANNELID##" => RandomJsonString(data.toString)

                case "##RANDOM_ITEMKIND1##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMETAG1##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMKIND_OFFKIND1##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMCHANNELID1##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMKIND2##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMETAG2##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMKIND_OFFKIND2##" => RandomJsonString(data.toString)
                case "##RANDOM_ITEMCHANNELID2##" => RandomJsonString(data.toString)
                case "{" => "{"
                case "[" => "["
                case "[{" => "[{"
                case _=> None
                }
              (x.toString.replace(data.toString,randomNum.toString))
              }.mkString("\n")
                  //logger.info(message)
                  i = i + 1
                  Thread.sleep(delay.toInt)
                }
     }

    def RandomJsonInt(x: String)= {  
                                    if (x == "##RANDOM_TOTALRESULT##" )
                                              random.nextInt(12344) 
                                    else if (x == "##RANDOM_RESULTPERPAGE##" )
                                              random.nextInt(12344)
                                  }

    def  RandomJsonString(x: String)= {
                            if (x == "##RANDOM_KINDS##" )
                                  "Search-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ETAGS##" )
                                  "SeachesTag-cf" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_NEXTPAGETOKEN##" )
                                  "ptoken-" + random.alphanumeric.take(6).mkString("*")
                            else if (x == "##RANDOM_REGIONCODE##" )
                                  "RC-" + random.alphanumeric.take(2).mkString("*")

                            else  if (x == "##RANDOM_ITEMKIND##" )
                                  "itemKind-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ITEMETAG##" )
                                  "itemEtag-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ITEMKIND_OFFKIND##" )
                                  "item_KOFK-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ITEMCHANNELID##" )
                                  "CID-" + random.alphanumeric.take(4).mkString("*")

                            else if (x == "##RANDOM_ITEMKIND1##" )
                                  "itemKind_1-" + random.alphanumeric.take(4).mkString("*")
                            else  if (x == "##RANDOM_ITEMETAG1##" )
                                  "itemEtag_1-" + random.alphanumeric.take(4).mkString("*")
                            else  if (x == "##RANDOM_ITEMKIND_OFFKIND1##" )
                                  "item_KOFK_1-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ITEMCHANNELID1##" )
                                  "CID_1-" + random.alphanumeric.take(4).mkString("*")

                            else if (x == "##RANDOM_ITEMKIND2##" )
                                  "itemKind-2-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ITEMETAG2##" )
                                  "itemEtag_2-" + random.alphanumeric.take(4).mkString("*")
                            else if (x == "##RANDOM_ITEMKIND_OFFKIND2##" )
                                  "item_KOFK_2-" + random.alphanumeric.take(4).mkString("*")
                            else  if (x == "##RANDOM_ITEMCHANNELID2##" )
                                  "CID_2-" + random.alphanumeric.take(4).mkString("*")
              }
}