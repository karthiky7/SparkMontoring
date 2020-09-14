package com.siemens.gbs.analytics 

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.parsing.json.JSONObject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.glue.AWSGlueClient
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.model.GetJobRunRequest
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import java.sql.Timestamp
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.TaskFailedReason
import org.apache.spark.TaskKilled
import org.apache.spark.Success
import org.apache.spark.ExceptionFailure
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import com.amazonaws.services.glue.util.JsonOptions

object Testone {
  def formatHeader(columnName:String)={
   var newColumnName = "([A-Z](?=[a-z]))".r.replaceAllIn(columnName, "_$1")
   newColumnName= "((?<=[a-z0-9])[A-Z])".r.replaceAllIn(newColumnName, "_$1").toLowerCase()
   newColumnName= "[^\\w]".r.replaceAllIn(newColumnName, "_")
   newColumnName= "^(_+)".r.replaceAllIn(newColumnName, "")
   newColumnName= "(_+)$".r.replaceAllIn(newColumnName, "")
   newColumnName= "(_{2,})".r.replaceAllIn(newColumnName, "_") 
   print("inside header method")
   newColumnName
  }
  def main(sysArgs: Array[String]){
     System.setProperty("hadoop.home.dir", "C:\\rpa-logs\\");
     val log = new GlueLogger()
      val sparkConfig=new SparkConf().setMaster("local").setAppName("tape-logdelivery-test-jr_e0dd661f15bef69dd1dd9320a53792726141e2260d947cee7713e1910eab1f67").set("spark.extraListeners", "com.siemens.gbs.analytics.MetricsManager1")
      
    val sparkContext: SparkContext = new SparkContext(sparkConfig)
    try{
      val glueContext: GlueContext = new GlueContext(sparkContext)
      glueContext.sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
      glueContext.sc.hadoopConfiguration.set("fs.s3a.access.key","ASIA6EQGT2XOU7")
      glueContext.sc.hadoopConfiguration.set("fs.s3a.secret.key","ekY7VsVlU+ONYcnzxk+xs/Wa7rkW0j")
      glueContext.sc.hadoopConfiguration.set("fs.s3a.session.token","FwoGZXIvYXdcQNdDh85R+vwyK6AXiH+5UqADe/GT/svjqvvpoBkWVgXIPJZHboMD5Glw854KdHo5sPsy3T7JImZjnUvJz0eZsPEoi6q/7n5kn9yLf5izrKIvztW7dMdytRGeKIbGYxpPPbC53fwwmPNoOdUNeXSf+u27jwbCzX+xNs/lE2ZsUDpaTRcPEn0XdhA0aRhkYA2wLKZs0LHoc8Wc9N6UWwyVDQkM79/JO1j8VbrEpWex6lYTvU0EKmtKLb52rzAu0a0KpC6NvWSijLu+35BTItExJ6S3p6shWpc69xD6ujmm7jBJ/szIKgFPsq35vYpK1GthMztMxmVD+IgqC+")
      //glueContext.sc.hadoopConfiguration.set("fs.s3a.server-side-encryption-algorithm","aws:kms")
      val sparkSession: SparkSession = glueContext.getSparkSession
 
      // specifying spark configuration
      sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold" , "-1")
      sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkSession.conf.set("spark.sql.broadcastTimeout",1200)
      sparkSession.conf.set("spark.network.timeout ",800)
      glueContext.sc.hadoopConfiguration.set("yarn.nodemanager.vmem-check-enabled","false")
      glueContext.sc.hadoopConfiguration.set("yarn.nodemanager.pmem-check-enabled","false")
      
      //glueContext.sc.addSparkListener(new MetricsManager1) 
      
      log.info("Intiated GlueContext and all job parameters")
      
       var contracts=sparkSession.read.option("delimiter",",").option("inferSchema","true").option("header","true").option("escape", "\"").option("quote", "\"").csv(s"s3://gbs-p2p-logistics-static-data-landing-dev/budget/")
      /* var bpaSession = glueContext.getSourceWithFormat(connectionType="s3",format="csv" , options = JsonOptions(Map("paths" -> Array("s3://gbs-sap-p2p-logistics-landing-dev/CONTRACTS/"), "compressionType" ->"gzip"))
                       ,formatOptions=JsonOptions(Map("separator" -> ";", "withHeader" -> true)) ,  transformationContext="bpaSession").getDynamicFrame()
      
      var df=bpaSession.toDF()*/
      contracts.columns.foreach(oldcolumnname => contracts = contracts.withColumnRenamed(oldcolumnname, formatHeader(oldcolumnname)))
      
      /*contracts.createOrReplaceTempView("Contract")
     
     val sqlexpression="""with a as (select distinct IFA,END_DATE,STATUS from Contract
                         |           where STATUS in ('ACTIVE','EXPIRED','PUBLISHED') and IFA not like  '' ),
                         |     b as (Select a.IFA,a.END_DATE,a.STATUS, ROW_NUMBER() Over (PARTITION BY a.IFA order by a.END_DATE desc) row_num  From a)
                         |select b.IFA,b.END_DATE,b.STATUS from b where b.row_num=1 """.stripMargin 
                         
     val curatedContractPvoCvoView=sparkSession.sql(sqlexpression)*/
      
      //contracts.repartition(1).write.mode(SaveMode.Append).format("parquet").save("C:\\rpa-logs\\test\\")
      contracts.repartition(1).write.mode(SaveMode.Append).format("parquet").save(s"s3://gbs-p2p-logistics-static-data-landing-dev/budget/")
      //log.info("count is "+df.count())
    }
        catch {
       case e: Exception =>{ 
         log.error(s"Glue job got failed : ${e.getStackTraceString}")
         throw e
       }
     
    }
    finally{
      log.info("Stopping spark contex")
      //Thread.sleep(300000)
      log.info("Stopping spark context")
      sparkContext.stop()
    }
  }
}
class MetricsManager1 extends SparkListener
{
        val key="ASIOHT4675GJ"
          val secret="BWLuca14qE/Y6vlN3KMRp"
          val token="FwoGZXIvYXdzEB7Lch/gWBWqXN3nYcYEn4+BeyfnINfMlaZmtcQK/Uv/CD6U4g3jKvpCR/RkSf+vi/5/JGUH5VXSC57TzxC9ovkr/ughZUKG+KCXOME8IqJHPvLF9uoMN3cSC5v6zYeGuXEkX+odSqr3+u8YwYfiX5cwdSha566Q2KxXikBrSdg6cTg/s4I3JHZ02lKpmi+e7WEX0UuEMky76fAGwE/szfvsaK2YzF6Hi92M8DEcvQ6TalXdCiLwOn5BTItjaVYiGSpeEarleMQFNelc0zBa+aEq60YmFhBLUHOgALUG4shwndKK4y72Mdi"
          val credentials = new BasicSessionCredentials(key,secret,token)
        val stageIds: collection.mutable.Map[String, Any] = collection.mutable.Map[String,Any]()
        var recordsWrittenCount = 0L
        var recordsReadCount = 0L
        var recordsWrittenSize = 0.0
        var recordsReadSize = 0.0
        var totalcores=0
        var ramSize=0.0
        val jsonData: collection.mutable.Map[String, Any]= collection.mutable.Map[String, Any]()
        val taskErrors: collection.mutable.Map[String, String]= collection.mutable.Map[String, String]()
        override def  onApplicationStart(appStart: SparkListenerApplicationStart){
          println(s"application Start Time is : ${appStart.time}")
          println(s"application Name is : ${appStart.appName}")
          val matchr=appStart.appName.substring(5)
          val (jobName,jobrunID)=(matchr.split("-jr")(0) , "jr"+matchr.split("-jr")(1))
          jsonData.put("jobName", jobName)
          jsonData.put("jobrunID", jobrunID)
          jsonData.put("ApplicationstartTime", appStart.time)
            /*val getJobRunRequest=new GetJobRunRequest()
            getJobRunRequest.setJobName(jobName)
            getJobRunRequest.setRunId(jobrunID)
            getJobRunRequest.setPredecessorsIncluded(false)
            val awsGlueClient=AWSGlueClient.builder().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion("eu-west-1").build()
            val jobmetadata=awsGlueClient.getJobRun(getJobRunRequest).getJobRun
            jsonData.put("DPU alloted", jobmetadata.getMaxCapacity)
            jsonData.put("jobTriggeredOn", jobmetadata.getStartedOn.toString())
            jsonData.put("JobArgumentes", JSONObject(jobmetadata.getArguments.asScala.toMap))*/
          savetoS3()
        }
        
        override def	onStageCompleted(stageCompleted:SparkListenerStageCompleted){
          
          synchronized{
            stageIds.put(stageCompleted.stageInfo.stageId.toString(), JSONObject(Map(("recordsRead",stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead),("recordsReadSizeinMB",stageCompleted.stageInfo.taskMetrics.inputMetrics.bytesRead.toDouble/(1024*1024)) ,("recordswrite",stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten) ,("recordsWriteSizeinMB",stageCompleted.stageInfo.taskMetrics.outputMetrics.bytesWritten.toDouble/(1024*1024)), ("numTasks",stageCompleted.stageInfo.numTasks))))
            recordsReadCount += stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead
            recordsWrittenCount += stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten
            recordsReadSize += stageCompleted.stageInfo.taskMetrics.inputMetrics.bytesRead.toDouble/(1024*1024)
            recordsWrittenSize += stageCompleted.stageInfo.taskMetrics.outputMetrics.bytesWritten.toDouble/(1024*1024)
          }
          println(s"the failure reason :${stageCompleted.stageInfo.failureReason}")
          if(stageCompleted.stageInfo.failureReason != None){
            jsonData.put("failureReason",stageCompleted.stageInfo.failureReason.get)
          }
          savetoS3()
        }
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd){
          println(taskEnd.taskInfo.successful)
          synchronized{
            if(!taskEnd.taskInfo.successful){
              val errorMessage=taskEnd.reason match{
                case Success => "Success"
                case k: TaskKilled => k.reason
                case e:ExceptionFailure => e.toErrorString
                case e:TaskFailedReason => e.toErrorString
                case other => "Unhandled task end reason"
              }
              taskErrors.put("stage"+taskEnd.stageId+"_task"+taskEnd.taskInfo.taskId+"_attempt"+taskEnd.taskInfo.attemptNumber,errorMessage)
            }
         }
        }
        override def onEnvironmentUpdate(environmentUpdate:SparkListenerEnvironmentUpdate){
          val env=environmentUpdate.environmentDetails.get("Spark Properties").get.toMap
          jsonData.put("TotalNumExecutors",env.get("spark.dynamicAllocation.maxExecutors").get)
          jsonData.put("TotalRam",env.get("spark.dynamicAllocation.maxExecutors").get.toInt*env.get("spark.executor.memory").get.substring(0, env.get("spark.executor.memory").get.length() - 1).toInt)
          jsonData.put("TotalCores",env.get("spark.dynamicAllocation.maxExecutors").get.toInt*env.get("spark.executor.cores").get.toInt)
         }
        override def  onApplicationEnd(appEnd: SparkListenerApplicationEnd){
          /*val client = AmazonDynamoDBClientBuilder.standard().build()
          val dynamoDB = new DynamoDB(client)
          val table = dynamoDB.getTable("ProductCatalog")*/
          jsonData.put("ApplicationEndTime", appEnd.time)
          savetoS3()
          
        }
        def savetoS3(){
          
          jsonData.put("stagesData", JSONObject(stageIds.toMap))
          jsonData.put("TotalRecordsRead", recordsReadCount)
          jsonData.put("TotalRecordsWrite", recordsWrittenCount)
         
          jsonData.put("TotalReadSizeinMB", recordsReadSize)
          jsonData.put("TotalWriteSizeinMB", recordsWrittenSize)
          jsonData.put("taskErrors", JSONObject(taskErrors.toMap))
          println(JSONObject(jsonData.toMap))
          //writing to s3
          val s3client = new AmazonS3Client()
          val in = new ByteArrayInputStream(JSONObject(jsonData.toMap).toString().getBytes("UTF-8"));
          val objectMetadata = new ObjectMetadata();
          objectMetadata.setSSEAlgorithm("aws:kms");
          val request = new PutObjectRequest("gbs-p2p-logistics-staging-dev", s"logs/metrics/${jsonData.get("jobrunID")}.json", in,objectMetadata);
          request.setMetadata(objectMetadata);
          //s3client.putObject(request)
        }
}
