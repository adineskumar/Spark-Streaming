-- Spark Streaming -- Streaming data from SLACK using SPARK
-- Reference       -- https://www.supergloo.com/fieldnotes/spark-streaming-example-from-slack/
--------------------------------------------------------------------------------------------------------------------------
-- Step:1 --  Create the folders in the below order

  mkdir spark-streaming-example
  cd spark-streaming-example
  mkdir src
  mkdir src/main
  mkdir src/main/scala
  mkdir src/main/scala/com
  mkdir src/main/scala/com/supergloo
  
-- Step:2 --  Create the "build.sbt" file in the root folder "spark-streaming-example"
          --  We need WebSocket, HttpClient and JSON parser to stream data from SLACK
          --  So the above mentioned dependencies should be mentioned in the build.sbt file

  name := "spark-streaming-example"
  version := "1.0"
  scalaVersion := "2.11.8"
  resolvers += "jitpack" at "https://jitpack.io"
  libraryDependencies ++= Seq("org.apache.spark" % "spark-streaming_2.11" % "1.6.1",
    "org.scalaj" %% "scalaj-http" % "2.2.1",
    "org.jfarcand" % "wcs" % "1.5")

-- Step:3 --  Create the SlackReceiver.scala file under "src/main/scala/com/supergloo" directory

          package com.supergloo

          import org.apache.spark.Logging
          import org.apache.spark.storage.StorageLevel
          import org.apache.spark.streaming.receiver.Receiver
          import org.jfarcand.wcs.{TextListener, WebSocket}

          import scala.util.parsing.json.JSON
          import scalaj.http.Http

          /**
          * Spark Streaming Example Slack Receiver from Slack
          */
          class SlackReceiver(token: String) extends Receiver[String](StorageLevel.MEMORY_ONLY) with Runnable with Logging {

            private val slackUrl = "https://slack.com/api/rtm.start"

            @transient
            private var thread: Thread = _

            override def onStart(): Unit = {
               thread = new Thread(this)
               thread.start()
            }

            override def onStop(): Unit = {
               thread.interrupt()
            }

            override def run(): Unit = {
               receive()
             }

            private def receive(): Unit = {
               val webSocket = WebSocket().open(webSocketUrl())
               webSocket.listener(new TextListener {
                 override def onMessage(message: String) {
                   store(message)
                 }
               })
            }

            private def webSocketUrl(): String = {
              val response = Http(slackUrl).param("token", token).asString.body
              JSON.parseFull(response).get.asInstanceOf[Map[String, Any]].get("url").get.toString
            }

          }

-- Step:4 --  Create the "SlackStreamingApp.scala" file under the "src/main/scala/com/supergloo" directory

                package com.supergloo

                import org.apache.spark.SparkConf
                import org.apache.spark.streaming.{Seconds, StreamingContext}

                /**
                  * Spark Streaming Example App
                  */
                object SlackStreamingApp {

                  def main(args: Array[String]) {
                    val conf = new SparkConf().setMaster(args(0)).setAppName("SlackStreaming")
                    val ssc = new StreamingContext(conf, Seconds(5))
                    val stream = ssc.receiverStream(new SlackReceiver(args(1)))
                    stream.print()
                    if (args.length > 2) {
                      stream.saveAsTextFiles(args(2))
                    }
                    ssc.start()
                    ssc.awaitTermination()
                  }

                }

-- Step:5 --  Compile the code using SBT
          -- Navigate to root folder "spark-streaming-example" and then type sbt compile
          -- SBT will download all the dependencies and it will create a jar file under "target" folder
          
-- Step:6 -- Go to " https://api.slack.com/docs/oauth-test-tokens" and get the access token from SLACK
          -- Go to root folder and type sbt, SBT will be launched
          -- Then type <run local[5] "access token" output>
          -- Then SBT will launch the streaming from SLACK using spark in standalone mode
          
 
