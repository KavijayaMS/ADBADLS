-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.secrets.get(scope="ADBADLSScope", key="dlstoken")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val configs = Map(
-- MAGIC   "fs.azure.account.auth.type" -> "OAuth",
-- MAGIC   "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
-- MAGIC   "fs.azure.account.oauth2.client.id" -> "73b7b0b7-9f6d-49fa-b332-aa1457e511f6",
-- MAGIC   "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="ADBADLSScope", key="dlstoken"),
-- MAGIC   "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "abfss://data@rachitstoragetraining.dfs.core.windows.net/",
-- MAGIC   mountPoint = "/mnt/data",
-- MAGIC   extraConfigs = configs)

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /mnt/data 

-- COMMAND ----------

-- MAGIC %md Text Analytics
-- MAGIC   

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import java.io._
-- MAGIC import java.net._
-- MAGIC import java.util._
-- MAGIC import javax.net.ssl.HttpsURLConnection
-- MAGIC import com.google.gson.Gson
-- MAGIC import com.google.gson.GsonBuilder
-- MAGIC import com.google.gson.JsonObject
-- MAGIC import com.google.gson.JsonParser
-- MAGIC import scala.util.parsing.json._
-- MAGIC 
-- MAGIC case class Language(documents: Array[LanguageDocuments], errors: Array[Any]) extends Serializable
-- MAGIC case class LanguageDocuments(id: String, detectedLanguages: Array[DetectedLanguages]) extends Serializable
-- MAGIC case class DetectedLanguages(name: String, iso6391Name: String, score: Double) extends Serializable
-- MAGIC 
-- MAGIC case class Sentiment(documents: Array[SentimentDocuments], errors: Array[Any]) extends Serializable
-- MAGIC case class SentimentDocuments(id: String, score: Double) extends Serializable
-- MAGIC 
-- MAGIC case class RequestToTextApi(documents: Array[RequestToTextApiDocument]) extends Serializable
-- MAGIC case class RequestToTextApiDocument(id: String, text: String, var language: String = "") extends Serializable
-- MAGIC 
-- MAGIC object SentimentDetector extends Serializable {
-- MAGIC     val accessKey = "06250262d4df4db8a5c0d51b95f4aba2"
-- MAGIC     val host = "https://adatrainingtextanalytics.cognitiveservices.azure.com/"
-- MAGIC     val languagesPath = "/text/analytics/v2.1/languages"
-- MAGIC     val sentimentPath = "/text/analytics/v2.1/sentiment"
-- MAGIC     val languagesUrl = new URL(host+languagesPath)
-- MAGIC     val sentimenUrl = new URL(host+sentimentPath)
-- MAGIC     val g = new Gson
-- MAGIC 
-- MAGIC     def getConnection(path: URL): HttpsURLConnection = {
-- MAGIC         val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
-- MAGIC         connection.setRequestMethod("POST")
-- MAGIC         connection.setRequestProperty("Content-Type", "text/json")
-- MAGIC         connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
-- MAGIC         connection.setDoOutput(true)
-- MAGIC         return connection
-- MAGIC     }
-- MAGIC 
-- MAGIC     def prettify (json_text: String): String = {
-- MAGIC         val parser = new JsonParser()
-- MAGIC         val json = parser.parse(json_text).getAsJsonObject()
-- MAGIC         val gson = new GsonBuilder().setPrettyPrinting().create()
-- MAGIC         return gson.toJson(json)
-- MAGIC     }
-- MAGIC 
-- MAGIC     def processUsingApi(request: RequestToTextApi, path: URL): String = {
-- MAGIC         val requestToJson = g.toJson(request)
-- MAGIC         val encoded_text = requestToJson.getBytes("UTF-8")
-- MAGIC         val connection = getConnection(path)
-- MAGIC         val wr = new DataOutputStream(connection.getOutputStream())
-- MAGIC         wr.write(encoded_text, 0, encoded_text.length)
-- MAGIC         wr.flush()
-- MAGIC         wr.close()
-- MAGIC 
-- MAGIC         val response = new StringBuilder()
-- MAGIC         val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
-- MAGIC         var line = in.readLine()
-- MAGIC         while (line != null) {
-- MAGIC             response.append(line)
-- MAGIC             line = in.readLine()
-- MAGIC         }
-- MAGIC         in.close()
-- MAGIC         return response.toString()
-- MAGIC     }
-- MAGIC 
-- MAGIC     def getLanguage (inputDocs: RequestToTextApi): Option[Language] = {
-- MAGIC         try {
-- MAGIC             val response = processUsingApi(inputDocs, languagesUrl)
-- MAGIC             val niceResponse = prettify(response)
-- MAGIC             val language = g.fromJson(niceResponse, classOf[Language])
-- MAGIC             if (language.documents(0).detectedLanguages(0).iso6391Name == "(Unknown)")
-- MAGIC                 return None
-- MAGIC             return Some(language)
-- MAGIC         } catch {
-- MAGIC             case e: Exception => return None
-- MAGIC         }
-- MAGIC     }
-- MAGIC 
-- MAGIC     def getSentiment (inputDocs: RequestToTextApi): Option[Sentiment] = {
-- MAGIC         try {
-- MAGIC             val response = processUsingApi(inputDocs, sentimenUrl)
-- MAGIC             val niceResponse = prettify(response)
-- MAGIC             val sentiment = g.fromJson(niceResponse, classOf[Sentiment])
-- MAGIC             return Some(sentiment)
-- MAGIC         } catch {
-- MAGIC             case e: Exception => return None
-- MAGIC         }
-- MAGIC     }
-- MAGIC }
-- MAGIC 
-- MAGIC val toSentiment = (textContent: String) =>
-- MAGIC         {
-- MAGIC             val inputObject = new RequestToTextApi(Array(new RequestToTextApiDocument(textContent, textContent)))
-- MAGIC             val detectedLanguage = SentimentDetector.getLanguage(inputObject)
-- MAGIC             detectedLanguage match {
-- MAGIC                 case Some(language) =>
-- MAGIC                     if(language.documents.size > 0) {
-- MAGIC                         inputObject.documents(0).language = language.documents(0).detectedLanguages(0).iso6391Name
-- MAGIC                         val sentimentDetected = SentimentDetector.getSentiment(inputObject)
-- MAGIC                         sentimentDetected match {
-- MAGIC                             case Some(sentiment) => {
-- MAGIC                                 if(sentiment.documents.size > 0) {
-- MAGIC                                     sentiment.documents(0).score.toString()
-- MAGIC                                 }
-- MAGIC                                 else {
-- MAGIC                                     "Error happened when getting sentiment: " + sentiment.errors(0).toString
-- MAGIC                                 }
-- MAGIC                             }
-- MAGIC                             case None => "Couldn't detect sentiment"
-- MAGIC                         }
-- MAGIC                     }
-- MAGIC                     else {
-- MAGIC                         "Error happened when getting language" + language.errors(0).toString
-- MAGIC                     }
-- MAGIC                 case None => "Couldn't detect language"
-- MAGIC             }
-- MAGIC         }

-- COMMAND ----------

-- MAGIC %scala print(toSentiment("Awesome Products purchased!"))

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.udf.register("toSentiment", toSentiment)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val data = Seq(("C10000", "Beast"),("C10001", "Awesome Product"), ("C10002", "Awful Products I have purchased this week!"), ("C10003", "Best Purchase")).toDF("customerid", "remarks")
-- MAGIC 
-- MAGIC data.createOrReplaceTempView("customer_remarks")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT customerid, remarks, toSentiment(remarks) as score
-- MAGIC FROM customer_remarks

-- COMMAND ----------

