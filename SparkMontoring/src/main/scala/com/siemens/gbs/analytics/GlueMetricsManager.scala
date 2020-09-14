package com.siemens.gbs.analytics

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.parsing.json.JSONObject

import org.apache.spark.ExceptionFailure
import org.apache.spark.Success
import org.apache.spark.TaskFailedReason
import org.apache.spark.TaskKilled
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerTaskEnd

import com.amazonaws.services.glue.AWSGlueClient
import com.amazonaws.services.glue.model.GetJobRunRequest
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest

class GlueMetricsManager extends SparkListener
{
        val stageIds: collection.mutable.Map[String, Any] = collection.mutable.Map[String,Any]()
        var recordsWrittenCount = 0L
        var recordsReadCount = 0L
        var recordsWrittenSize = 0.0
        var recordsReadSize = 0.0
        val jsonData: collection.mutable.Map[String, Any]= collection.mutable.Map[String, Any]()
        val taskErrors: collection.mutable.Map[String, String]= collection.mutable.Map[String, String]()
        override def  onApplicationStart(appStart: SparkListenerApplicationStart){
          println(s"application Start Time is : ${appStart.time}")
          println(s"application Name is : ${appStart.appName}")
          val matchr=appStart.appName.substring(5)
          val (jobName,jobrunID)=(matchr.split("-jr")(0) , "jr"+matchr.split("-jr")(1))
          jsonData.put("jobName", jobName)
          jsonData.put("jobrunID", jobrunID)
          jsonData.put("ApplicationExcecutionstartTime", appStart.time)
            val getJobRunRequest=new GetJobRunRequest()
            getJobRunRequest.setJobName(jobName)
            getJobRunRequest.setRunId(jobrunID)
            getJobRunRequest.setPredecessorsIncluded(false)
            val awsGlueClient=AWSGlueClient.builder().withRegion("eu-west-1").build()
            val jobmetadata=awsGlueClient.getJobRun(getJobRunRequest).getJobRun
            jsonData.put("DPU alloted", jobmetadata.getMaxCapacity)
            jsonData.put("jobTriggeredOn", jobmetadata.getStartedOn.toString())
            jsonData.put("JobArgumentes", JSONObject(jobmetadata.getArguments.asScala.toMap))
          savetoS3()
          Thread.sleep(5000)
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
            jsonData.put("stage"+stageCompleted.stageInfo.stageId+"_failureReason",stageCompleted.stageInfo.failureReason.get)
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
          jsonData.put("ApplicationExcecutionEndTime", appEnd.time)
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
          val request = new PutObjectRequest("gbs-p2p-logistics-staging-dev", s"logs/metrics/${jsonData.get("jobrunID").get}.json", in,objectMetadata);
          request.setMetadata(objectMetadata);
          s3client.putObject(request)
        }
}
