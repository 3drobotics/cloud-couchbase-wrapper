import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, Sink}
import akka.util.ByteString
import com.couchbase.client.java.bucket.BucketFlusher
import com.couchbase.client.java.error.{CASMismatchException, DocumentDoesNotExistException}
import com.couchbase.client.java.view.{DefaultView, DesignDocument, Stale}
import com.typesafe.config.ConfigFactory
import io.dronekit.{DocumentNotFound, CouchbaseStreamsWrapper}
import org.scalatest._
import spray.json.DefaultJsonProtocol

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.languageFeature.postfixOps
import scala.util.Random


/**
 * Created by Jason Martens on 9/25/15.
 *
 * Integration test class for CouchbaseStreamsWrapper, requires a running Couchbase instance
 */
class IntegrationTest extends WordSpec with Matchers {
  // To make marshalling and unmarshalling work
  case class TestEntity(id: String = UUID.randomUUID().toString, name: String, age: Long, sex: Option[String], cas: Option[Long] = None)

  class Protocol extends DefaultJsonProtocol {
    implicit val testEntityFormat = jsonFormat5(TestEntity)
  }
  val protocol = new Protocol()

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val context: ExecutionContext = system.dispatcher

  val couchbaseConfig = ConfigFactory.load().getConfig("couchbase")
  val couchbase = new CouchbaseStreamsWrapper(
    couchbaseConfig.getString("hostname"),
    couchbaseConfig.getString("bucket"),
    couchbaseConfig.getString("password"),
    protocol)

  import protocol._

  def flushBuckets(): Unit = {
    BucketFlusher.flush(
      couchbase.cluster.core(),
      couchbaseConfig.getString("bucket"),
      couchbaseConfig.getString("password")
    ).toBlocking.first()
  }
  flushBuckets()

  def createIndex(designDocName: String, viewName: String, mapFunction: String, reduceFunction: String): Unit = {
    val tokenView =
      if (reduceFunction.isEmpty) DefaultView.create(viewName, mapFunction)
      else DefaultView.create(viewName, mapFunction, reduceFunction)
    val designDoc = DesignDocument.create(designDocName, List(tokenView))
    couchbase.bucket.bucketManager().upsertDesignDocument(designDoc, false)
  }

  val nameMap = "function (doc, meta) {if (doc.name) {emit(doc.name, null);} }"
  createIndex("NameDoc", "ByName", nameMap, "")
  val nameAgeMap = "function (doc, meta) {if (doc.name && doc.age) {emit([doc.name, doc.age], null);} }"
  createIndex("NameAndAgeDoc", "ByNameAndAge", nameAgeMap, "")


  "Should be able to insert and retrieve a document" in {
    val entity = TestEntity(name = "Jill", age = 23, sex = None)
    val insertFuture = couchbase.insertDocument[TestEntity](entity, entity.id)
    Await.ready(insertFuture, 10 seconds)
    val getFuture = couchbase.lookupByKey[TestEntity](entity.id)
    val result = Await.result(getFuture, 10 seconds)
    result.cas should not be 0
    result.entity shouldBe entity
  }

  "Should throw an exception if retrieving a non-existing document" in {
    intercept[DocumentNotFound] {
      Await.result(couchbase.lookupByKey("Unicorn"), 1 seconds) shouldBe None
    }
  }

  "Should be able to update an existing document" in {
    val entity = TestEntity(name = "Doris", age = 66, sex = Some("Female"))
    val insertFuture = couchbase.insertDocument[TestEntity](entity, entity.id)
    val result = Await.result(insertFuture, 10 seconds)
    result.entity shouldBe entity
    val returnedEntity = result.entity
    val replacedFuture = couchbase.replaceDocument[TestEntity](
      entity.copy(age = 67), returnedEntity.id, result.cas)
    val updatedResult = Await.result(replacedFuture, 10 seconds)
    updatedResult.entity should not be None
    updatedResult.cas should not be result.cas
    updatedResult.entity.age shouldBe 67
  }

  "Should prevent updating a document without a valid CAS" in {
    val entity = TestEntity(name = "Jack", age = 3, sex = Some("Male"))
    val insertFuture = couchbase.insertDocument[TestEntity](entity, entity.id)
    val result = Await.result(insertFuture, 10 seconds)
    result should not be None
    val returnedEntity = result.entity
    val replacedFuture = couchbase.replaceDocument[TestEntity](
      returnedEntity.copy(id = returnedEntity.id, name = "Jill", age = 22, sex = None),
      returnedEntity.id,
      1)
    intercept[CASMismatchException] {
      Await.result(replacedFuture, 10 seconds)
    }
  }

  "Should remove documents by ID" in {
    val entity = TestEntity(name = "Jack", age = 3, sex = Some("Male"))
    val insertFuture = couchbase.insertDocument[TestEntity](entity, entity.id)
    val result = Await.result(insertFuture, 10 seconds)
    result should not be None
    val removeFuture = couchbase.removeByKey(entity.id)
    Await.ready(removeFuture, 10 seconds)
  }

  "Should throw an error if removing a non-existing document" in {
    intercept[DocumentDoesNotExistException] {
      Await.result(couchbase.removeByKey("aslkdf"), 10 seconds)
    }
  }

  "Should get a list of results when querying" in {
    val personOne = TestEntity(name = "Bonnie", age = 25, sex = Some("Female"))
    val personTwo = TestEntity(name = "Clyde", age = 24, sex = Some("Male"))
    val f1 = couchbase.insertDocument[TestEntity](personOne, personOne.id)
    val f2 = couchbase.insertDocument[TestEntity](personTwo, personTwo.id)
    Await.ready(f1 zip f2, 10 seconds)

    val source = couchbase.indexQueryToEntity[TestEntity](
      "NameDoc", "ByName", List(personOne.name, personTwo.name), stale = Stale.FALSE)
    val result = Await.result(source.grouped(2).runWith(Sink.head), 10 seconds).map(_.entity)
    result.find(_.name == personOne.name) shouldBe Some(personOne)
    result.find(_.name == personTwo.name) shouldBe Some(personTwo)
  }

  "Queries for non-existing things should throw a NoSuchElementException" in {
    intercept[NoSuchElementException] {
      val s = couchbase.indexQueryToEntity[TestEntity]("NameDoc", "ByName", List("Unicorn"), stale = Stale.FALSE)
      Await.result(s.grouped(1000).runWith(Sink.head), 10 seconds).map(println)
    }
  }

  "Should be able to query for multiple keys" in {
    val personOne = TestEntity(name = "Big Bird", age = 4, sex = Some("Unknown"))
    val personTwo = TestEntity(name = "Big Bird", age = 3, sex = Some("Unknown"))
    val f1 = couchbase.insertDocument[TestEntity](personOne, personOne.id)
    val f2 = couchbase.insertDocument[TestEntity](personTwo, personTwo.id)
    Await.ready(f1 zip f2, 10 seconds)

    val source = couchbase.compoundIndexQueryToEntity[TestEntity](
      "NameAndAgeDoc", "ByNameAndAge", List(List(personOne.name, personOne.age)), Stale.FALSE)
    val result = Await.result(source.grouped(2).runWith(Sink.head), 10 seconds).map(_.entity)
    result.head shouldBe personOne
    result.length shouldBe 1
  }

  "Should be able to look up a single document" in {
    val person = TestEntity(name = "Emily Haines", age = 41, sex = Some("Female"))
    Await.ready(couchbase.insertDocument[TestEntity](person, person.id), 10 seconds)
    val rowFuture = couchbase.indexQuerySingleElement("NameDoc", "ByName", person.name, forceIndex = true).flatMap {
        case Some(asyncRow) => couchbase.getEntityFromRow[TestEntity](asyncRow)
        case None => throw new DocumentDoesNotExistException()}
    val rowResult = Await.result(rowFuture, 10 seconds)
    rowResult.entity shouldBe person
  }

  "Should be able to batch lookups by key" in {
    val personOne = TestEntity(name = "Grover", age = 4, sex = Some("male"))
    val personTwo = TestEntity(name = "Abby Cadabby", age = 4, sex = Some("Female"))
    val f1 = couchbase.insertDocument[TestEntity](personOne, personOne.id)
    val f2 = couchbase.insertDocument[TestEntity](personTwo, personTwo.id)
    Await.ready(f1 zip f2, 10 seconds)

    val source = couchbase.batchLookupByKey[TestEntity](List(personOne.id, personTwo.id))
    val results = Await.result(source.grouped(2).runWith(Sink.head), 10 seconds)
    results.find(_.entity.id == personOne.id).get.entity shouldBe personOne
    results.find(_.entity.id == personTwo.id).get.entity shouldBe personTwo
  }

  "Should be able to insert and retrieve binary documents" in {
    val data = ByteString(Random.alphanumeric.take(100).map(_.toByte).toArray[Byte])
    val insertFuture = couchbase.binaryInsertDocument(data, "binaryDoc1")
    val result = Await.result(insertFuture, 1 second)
    assert(result.cas > 0)
    val readFuture = couchbase.binaryLookupByKey("binaryDoc1")
    val readResult = Await.result(readFuture, 1 second)
    readResult.entity shouldBe data
  }



}
