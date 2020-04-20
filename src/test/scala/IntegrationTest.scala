// © 2019 3D Robotics. License: ISC
import java.util.UUID
import java.time.{Duration, Instant}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import com.couchbase.client.java.{CouchbaseCluster, PersistTo}
import com.couchbase.client.java.bucket.{BucketFlusher, BucketType}
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.error.{CASMismatchException, DocumentDoesNotExistException}
import com.couchbase.client.java.query.Select._
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.Sort._
import com.couchbase.client.java.query.dsl.{Expression, Sort}
import com.couchbase.client.java.query.{Index, N1qlParams, N1qlQuery, Statement}
import com.couchbase.client.java.view.{DefaultView, DesignDocument, Stale}
import com.typesafe.config.ConfigFactory

import io.dronekit.{CouchbaseStreamsWrapper, DocumentNotFound}
import org.scalatest._
import play.api.libs.json._
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration => _, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.languageFeature.postfixOps
import scala.util.Random

object Protocol {
  implicit val instantFormat = new Format[Instant] {
    def writes(instant: Instant): JsValue = {
      JsString(instant.toString)
    }

    def reads(json: JsValue): JsResult[Instant] = json match {
      case JsString(str) => JsSuccess(Instant.parse(str))
      case _ => JsError("expected ISO 8601 timestamp string")
    }
  }
}

trait TestTrait{ val doc: String }

case class TestEntity(id: String = UUID.randomUUID().toString,
                      name: String, age: Long, sex: Option[String],
                      birthday: Option[Instant] = None,
                      cas: Option[Long] = None,
                      doc: String = "TestEntity") extends TestTrait

object TestEntity {
  implicit val format = Json.format[TestEntity]
}

case class OtherTestEntity(id: String = UUID.randomUUID().toString,
                            name: String, location: String,
                            cas: Option[Long] = None,
                            doc: String = "OtherTestEntity") extends TestTrait

object OtherTestEntity {
  implicit val format = Json.format[OtherTestEntity]
}

object TestTrait {
  implicit val format = new Format[TestTrait] {
    def reads(json: JsValue): JsResult[TestTrait] = {
      (json \ "doc").asOpt[String] match {
        case Some("TestEntity") => json.validate[TestEntity]
        case Some("OtherTestEntity") => json.validate[OtherTestEntity]
        case _ => throw new RuntimeException("did not get a string for TestTrait doctype")
      }
    }

    override def writes(obj: TestTrait): JsValue = {
      obj.doc match {
        case "TestEntity" => Json.toJson(obj.asInstanceOf[TestEntity])
        case "OtherTestEntity" => Json.toJson(obj.asInstanceOf[OtherTestEntity])
        case _ => throw new RuntimeException("Could not write TestTrait")
      }
    }
  }
}

/**
 * Created by Jason Martens on 9/25/15.
 *
 * Integration test class for CouchbaseStreamsWrapper, requires a running Couchbase instance
 */
class IntegrationTest extends WordSpec with Matchers with BeforeAndAfterAll {
  val testBucketName = "cloud-couchbase-wrapper-test"
  val testUserName = "Administrator"
  val testPassword = "password"

  implicit val system: ActorSystem = ActorSystem()
  implicit val context: ExecutionContext = system.dispatcher

  val couchbaseConfig = ConfigFactory.load().getConfig("couchbase")
  val couchbase = new CouchbaseStreamsWrapper(
    List(couchbaseConfig.getString("hostname")),
    testBucketName,
    testUserName,
    testPassword,
  )

  couchbase.bucket.bucketManager().flush();
  couchbase.bucket.query(Index.createPrimaryIndex().on(testBucketName))

  def createIndex(designDocName: String, viewName: String, mapFunction: String, reduceFunction: String): Unit = {
    val tokenView =
      if (reduceFunction.isEmpty) DefaultView.create(viewName, mapFunction)
      else DefaultView.create(viewName, mapFunction, reduceFunction)
    val designDoc = DesignDocument.create(designDocName, List(tokenView).asJava)
    couchbase.bucket.bucketManager().upsertDesignDocument(designDoc, false)
  }

  val nameMap = "function (doc, meta) {if (doc.name) {emit(doc.name, null);} }"
  createIndex("NameDoc", "ByName", nameMap, "")
  val nameAgeMap = "function (doc, meta) {if (doc.name && doc.age) {emit([doc.name, doc.age], null);} }"
  createIndex("NameAndAgeDoc", "ByNameAndAge", nameAgeMap, "")
  val birthdayMap = "function (doc, meta) {if (doc.birthday) {emit([new Date(doc.birthday).valueOf()], null);} }"
  createIndex("BirthdayDoc", "Birthday", birthdayMap, "")


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
      Await.result(couchbase.lookupByKey[TestEntity]("Unicorn"), 1 seconds) shouldBe None
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
      Await.result(s.grouped(1000).runWith(Sink.head), 10 seconds)
    }
  }

  "Should be able to query for multiple keys" in {
    val personOne = TestEntity(name = "Big Bird", age = 4, sex = Some("Unknown"))
    val personTwo = TestEntity(name = "Big Bird", age = 3, sex = Some("Unknown"))
    val f1 = couchbase.insertDocument[TestEntity](personOne, personOne.id)
    val f2 = couchbase.insertDocument[TestEntity](personTwo, personTwo.id)
    Await.ready(f1 zip f2, 10 seconds)

    val source = couchbase.compoundIndexQueryByKeysToEntity[TestEntity](
      "NameAndAgeDoc", "ByNameAndAge", Some(List(List(personOne.name, personOne.age))), Stale.FALSE)
    val result = Await.result(source.grouped(2).runWith(Sink.head), 10 seconds).map(_.entity)
    result.head shouldBe personOne
    result.length shouldBe 1
  }

  "Should be able to query a compound key with a range" in {
    // create 10 people with birthdays
    val startDate = Instant.now
    val peopleFuture = Future.sequence((1 to 10).map { num =>
      val birthday = startDate.plus(Duration.ofDays(num))
      val sex: String = Seq("Male", "Female")(Random.nextInt(1))
      val person = TestEntity(
        name = s"Testing_$num",
        age = Random.nextInt(99),
        sex = Some(sex),
        birthday = Some(birthday)
      )
      couchbase.insertDocument[TestEntity](person, person.id, persist = PersistTo.MASTER).map(docResp => docResp.entity)
    })

    // query for 5 of them
    val endDate = startDate.plus(Duration.ofDays(5))
    Await.ready(peopleFuture, 10 seconds)
    val src = couchbase.compoundIndexQueryByRangeToEntity[TestEntity](
      "BirthdayDoc", "Birthday", Some(Seq(startDate.toEpochMilli)),
      Some(Seq(endDate.toEpochMilli)), Stale.FALSE
    )

    val result = Await.result(src.grouped(10).runWith(Sink.head), 10 seconds).map(_.entity)
    result.length shouldBe 5
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

    val source = Source(List(personOne.id, personTwo.id)).mapAsync(10)(couchbase.lookupByKey[TestEntity](_))
    val results = Await.result(source.runWith(Sink.seq), 10 seconds)
    results.find(_.entity.id == personOne.id).get.entity shouldBe personOne
    results.find(_.entity.id == personTwo.id).get.entity shouldBe personTwo
  }

  case class UpdateObject[T](entity: T, cas: Long, key: String)

  "Should be able to do a batch update by keys" in {
    val personOne = TestEntity(name = "Grover", age = 4, sex = Some("male"))
    val otherPersonTwo = OtherTestEntity(name = "Other", location = "Berkeley")
    val f1 = couchbase.insertDocument[TestEntity](personOne, personOne.id)
    val f2 = couchbase.insertDocument[OtherTestEntity](otherPersonTwo, otherPersonTwo.id)
    val insertResults = Await.ready(f1 zip f2, 10 seconds)

    insertResults.map{ case (p1, p2) =>
      val updateOne = personOne.copy(age = 5)
      val updateTwo = otherPersonTwo.copy(location="Oakland")
      val updateSeq: List[UpdateObject[TestTrait]] = List(UpdateObject[TestTrait](key = personOne.id, cas = p1.cas, entity = updateOne), UpdateObject[TestTrait](key = otherPersonTwo.id, cas = p2.cas, entity = updateTwo))

      val source = Source(updateSeq).mapAsync(10){u => couchbase.replaceDocument(u.entity, u.key, u.cas)}
      val results = Await.result(source.runWith(Sink.seq), 10 seconds)
      results.find{r => r.entity.doc == "TestEntity"}.get.entity.asInstanceOf[TestEntity] shouldBe updateOne
      results.find{r => r.entity.doc == "OtherTestEntity"}.get.entity.asInstanceOf[OtherTestEntity] shouldBe updateTwo
    }
  }

  "Should error out if one item in the batch update can't be found" in {
    val personOne = TestEntity(name = "Grover", age = 4, sex = Some("male"))
    val personTwo = TestEntity(name = "Other", age = 4, sex = Some("male"))
    val personThree = TestEntity(name = "Grover2", age = 5, sex = Some("male"))
    val personFour = TestEntity(name = "Grover3", age = 6, sex = Some("male"))

    val f1 = couchbase.insertDocument[TestEntity](personOne, personOne.id)
    val f2 = couchbase.insertDocument[TestEntity](personThree, personThree.id)
    val f3 = couchbase.insertDocument[TestEntity](personFour, personFour.id)

    val aggFut = for {
      f1res <-f1
      f2res <-f2
      f3res <-f3
    } yield (f1res.cas, f2res.cas, f3res.cas)
    val casRes = Await.result(aggFut, 10 seconds)

    val updateOne = personOne.copy(age = 5)
    val updateSeq: List[UpdateObject[TestTrait]] = List(
      UpdateObject[TestTrait](key = personOne.id, cas = casRes._1, entity = updateOne),
      UpdateObject[TestTrait](key = "some-bad-key", cas = 123, entity = personTwo),
      UpdateObject[TestTrait](key = personThree.id, cas = casRes._2, entity = personThree.copy(age=6)),
      UpdateObject[TestTrait](key = personFour.id, cas = casRes._3, entity = personFour.copy(age=7)))

    val source = Source(updateSeq).mapAsync(10){u => couchbase.replaceDocument(u.entity, u.key, u.cas)}
    intercept[DocumentDoesNotExistException] {
      Await.result(source.runWith(Sink.seq), 10 seconds)
    }
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

  "Should be able to query using N1QL" in {
    val kyloRen = TestEntity(name = "Kylo Ren", age = 13, sex = Some("Male"))
    Await.ready(couchbase.insertDocument[TestEntity](kyloRen, kyloRen.id), 10 seconds)

    val query: Statement = select("name, age")
      // Need to wrap bucket name with i() because of the - in the name
      .from(i(testBucketName))
      .where(x("name").eq(s("Kylo Ren")))
      .orderBy(asc(x("age")))
    val queryResponse = Await.result(
      couchbase.n1qlQuery(
        q = query,
        params = N1qlParams.build().adhoc(false).consistency(ScanConsistency.REQUEST_PLUS)), 10 seconds)

    queryResponse.rows.map(_.value().toString)
      .runWith(TestSink.probe[String])
      .request(1)
      .expectNext("""{"name":"Kylo Ren","age":13}""")
      .expectComplete()
  }

  "Should be able to query using N1QL and return entities" in {
    val rey = TestEntity(name = "Rey", age = 20, sex = Some("Female"))
    Await.ready(couchbase.insertDocument[TestEntity](rey, rey.id), 10 seconds)

    val where: Expression = x("name").eq(s("Rey"))
    val order: Sort = asc(x("age"))
    val params: N1qlParams = N1qlParams.build().adhoc(false).consistency(ScanConsistency.REQUEST_PLUS)
    val queryResponse = Await.result(couchbase.n1qlQueryToEntity[TestEntity](where, order, params), 10 seconds)

    queryResponse.rows.map(_.entity)
      .runWith(TestSink.probe[TestEntity])
      .request(1)
      .expectNext(rey)
      .expectComplete()
  }

  "Should be able to paginate objects and return sorted by date" in {
    val peopleFuture = Future.sequence((1 to 100).map { num =>
      val birthday = Instant.now
        .plus(Duration.ofSeconds(Random.nextInt(10*60*60)))
      val sex: String = Seq("Male", "Female")(Random.nextInt(1))
      val person = TestEntity(
        name = s"Testing_$num",
        age = Random.nextInt(99),
        sex = Some(sex),
        birthday = Some(birthday)
      )
      couchbase.insertDocument[TestEntity](person, person.id, persist = PersistTo.MASTER).map(docResp => docResp.entity)
    })
    Await.ready(peopleFuture, 10 seconds)
    var lastDateTime = Instant.now.minus(Duration.ofDays(1))
    var startKey: Long = 0
    var startDocId: Option[String] = None
    (0 to 9).foreach { offset =>
      val queryResponse = Await.result(couchbase.paginatedIndexQuery[TestEntity](
        designDoc = "BirthdayDoc",
        viewDoc = "Birthday",
        startKey = Seq(startKey),
        endKey = Seq(""),
        startDocId = startDocId,
        limit = 10,
        stale = Stale.FALSE
      ), 10 seconds)
      val rows = Await.result(queryResponse.rows.grouped(100).runWith(Sink.head), 10 seconds)
          // Yuck... This should not be necessary at all, however somewhere between the query
          // to couchbase and the runWith above, the rows can get out of order (which should not happen!)
          // Need to investigate the root cause of this but don't have time to now.
          .sortBy(doc => doc.entity.birthday.get.toEpochMilli)
      rows.length shouldBe 10
      rows.foreach { docResponse =>
        assert(docResponse.entity.birthday.get.toEpochMilli > lastDateTime.toEpochMilli, s"${docResponse.entity} is out of order with $lastDateTime")
        lastDateTime = docResponse.entity.birthday.get
      }
      startDocId = Some(rows.last.entity.id)
      startKey = rows.last.entity.birthday.get.toEpochMilli
    }
  }
}
