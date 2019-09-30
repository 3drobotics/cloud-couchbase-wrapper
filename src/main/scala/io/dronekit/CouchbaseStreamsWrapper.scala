package io.dronekit

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 9/16/15.
 *
 */

import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.couchbase.client.deps.io.netty.buffer._
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil
import com.couchbase.client.java.{ReplicateTo, PersistTo, CouchbaseCluster, Bucket}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.document.{BinaryDocument, JsonDocument, RawJsonDocument}
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.query.Select._
import com.couchbase.client.java.query._
import com.couchbase.client.java.query.dsl.Expression._
import com.couchbase.client.java.query.dsl.{Expression, Sort}
import com.couchbase.client.java.view.{AsyncViewRow, Stale, ViewQuery}
import com.couchbase.client.java.error.TemporaryFailureException
import com.couchbase.client.core.time.Delay
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import rx.RxReactiveStreams
import rx.{Observable, Subscriber}
import scala.concurrent.{Future, Promise}
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Created by Jason Martens <jason.martens@3drobotics.com> on 7/31/15.
 *
 * Wrapper class to convert the RxJava interface to Akka-Streams
 *
 */


/**
  * Wrapper to allow returning the CAS value of an object along with the object itself
  *
  * @param cas The current Check And Set value of the queried object
  * @param entity The contents of the document
  * @tparam T The type of the contents
  */
case class DocumentResponse[T](cas: Long, entity: T)

/**
  * Wraps returned values in a DocumentResponse
  *
  * @param rows A Source of DocumentResponse objects which includes the CAS value
  * @param status The status of the query as reported by Couchbase
  * @param errors Any errors from the query as reported by Couchbase
  * @param info Metrics on the query as reported by Couchbase
  * @tparam T The type of the entity
  */
case class QueryResponse[T](rows: Source[DocumentResponse[T], Any],
                         status: Future[String],
                         errors: Future[JsonObject],
                         info: Future[N1qlMetrics])

/**
  * Wraps returned values in a DocumentResponse
  *
  * @param rows A Source of DocumentResponse objects which includes the CAS value
  * @param errors Any errors from the query as reported by Couchbase
  * @param debug Metrics on the query as reported by Couchbase
  * @tparam T The type of the entity
  */
case class ViewQueryResponse[T](rows: Source[DocumentResponse[T], Any],
                            errors: Future[JsonObject],
                            debug: JsonObject)

/**
  * Returned values are AsyncN1qlQueryRow(s), no CAS value is returned unless the query included it
  *
  * @param rows The results of the query
  * @param status The status of the query as reported by Couchbase
  * @param errors Any errors from the query as reported by Couchbase
  * @param info Metrics on the query as reported by Couchbase
  */
case class RawQueryResponse(rows: Source[AsyncN1qlQueryRow, Any],
                         status: Future[String],
                         errors: Future[JsonObject],
                         info: Future[N1qlMetrics])

class DocumentNotFound(message: String) extends RuntimeException(message)
class DeserializationError(couchbaseKey: String, exception: JsResultException) extends RuntimeException(s"Deserialization error with couchbase key '${couchbaseKey}': ${exception}")

object CouchbaseStreamsWrapper {
  // Keep the env in an object so it is only created once
  val env = DefaultCouchbaseEnvironment.builder().build()

  val temporaryFailureRetryMs = 50
  val temporaryFailureMaxRetries = 5

}

trait JsonSerializer[T] {
  def parse(s: String): T
  def serialize(v: T): String
}

object JsonSerializer {
  implicit def playCompat[T](implicit fmt: play.api.libs.json.Format[T]) = new JsonSerializer[T] {
    def parse(s: String): T = Json.parse(s).as[T]
    def serialize(v: T): String = Json.stringify(fmt.writes(v))
  }
}


/**
  * A wrapper around a specific bucket which provides an Akka-Streams translation to the RxJava couchbase java API.
  *
  * @param hosts Hostname of the couchbase server to connect to
  * @param bucketName Name of the bucket to connect to
  * @param password The password for the bucket
  * @param ec Execution Context for Futures and Streams
  * @param mat Materializer for Akka-Streams
  */

class CouchbaseStreamsWrapper(hosts: List[String], bucketName: String, userName: String, password: String)
                    (implicit ec: ExecutionContext, mat: ActorMaterializer)
{

  // legacy Couchbase setup method - assumes the bucket name and user name are identical
  def this(host: String, bucketName: String, password: String)(implicit ec: ExecutionContext, mat: ActorMaterializer) {
    this(List(host), bucketName, bucketName, password)
  }

  // Reuse the env here
  val cluster = CouchbaseCluster.create(CouchbaseStreamsWrapper.env, hosts.asJava)
  cluster.authenticate(userName, password)
  val bucket = cluster.openBucket(bucketName)
  val log: Logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Convert an Observable with 0 or 1 element into an Option[Future]
   *
   * @param observable The source to convert
   * @tparam T The type of the resulting Future
   * @return The future
   */
  private def observableToOptionFuture[T](observable: Observable[T]): Future[Option[T]] = {
    val p = Promise[Option[T]]
    observable.subscribe(new Subscriber[T]() {
      override def onCompleted(): Unit = p.trySuccess(None)
      override def onError(e: Throwable): Unit = p.tryFailure(e)
      override def onNext(t: T): Unit = p.trySuccess(Some(t))
    })
    p.future
  }

  /**
    * Convert an Observable with a single element into a Future
    *
    * @param observable The source to convert
    * @tparam T The type of the resulting Future
    * @return The future
    */
  private def observableToFuture[T](observable: Observable[T]): Future[T] = {
    val p = Promise[T]
    observable.single.subscribe(new Subscriber[T]() {
      override def onCompleted(): Unit = p.tryFailure(new NoSuchElementException())
      override def onError(e: Throwable): Unit = p.tryFailure(e)
      override def onNext(t: T): Unit = p.trySuccess(t)
    })
    p.future
  }

  /**
   * Convert an RxJava observable into an Akka-Streams Source
    *
    * @param observable The observable to convert from
   * @tparam T The type parameter of the observable/source
   * @return An Akka-Streams Source
   */
  private def observableToSource[T](observable: Observable[T]): Source[T, Any] = {
    Source.fromPublisher(RxReactiveStreams.toPublisher(observable))
  }


  /**
   * Using play-json, convert the found document into an entity of type T
    *
    * @param docObservable The observable to get the json document from
   * @param format The json format implicit to use for conversion
   * @tparam T The type parameter to convert to
   * @return A DocumentResponse with the entity and the current CAS value
   */
  private def convertToEntity[T](docObservable: Observable[RawJsonDocument])
                                (implicit format: JsonSerializer[T]): Future[DocumentResponse[T]] = {
    observableToOptionFuture(docObservable).map(_.getOrElse(throw new DocumentNotFound(""))).map(convertToEntity(_))
  }

  /**
   * Using play-json, convert the found document into an entity of type T
    *
    * @param jsonDocument The document retrieved
   * @param format The json format implicit to use for conversion
   * @tparam T The type parameter to convert to
   * @return A DocumentResponse with the entity and the current CAS value
   */
  private def convertToEntity[T](jsonDocument: RawJsonDocument)(implicit format: JsonSerializer[T]): DocumentResponse[T] = {
    try {
      val entity = format.parse(jsonDocument.content())
      DocumentResponse(cas = jsonDocument.cas(), entity = entity)
    } catch {
      case e: JsResultException => throw new DeserializationError(
        couchbaseKey = jsonDocument.id, exception = e
      )
    }
  }

  /**
   * Replace a document in couchbase with the given entity by marshalling it to
   * json, then returning the unmarshalled json returned from couchbase
    *
    * @param entity Overwrite the document in the database with this entity
   * @param key The document to overwrite
   * @param expiry optional expiry time in seconds, 0 (stored indefinitely) if not set
   * @param cas compare-and-swap value
   * @tparam T The type of the entity
   * @return The replaced document in the database
   */
  def replaceDocument[T](entity: T, key: String, cas: Long, expiry: Int = 0)(implicit format: JsonSerializer[T]): Future[DocumentResponse[T]] = {
    val jsonString = format.serialize(entity)
    val doc = RawJsonDocument.create(key, expiry, jsonString, cas)
    val replaceObservable = bucket.async().replace(doc)
    convertToEntity[T](replaceObservable)
  }

  /**
   * Marshall entity to json, insert it into the database, then unmarshal the response and return it
    *
    * @param entity The object to insert
   * @param key The location to insert it to
   * @param expiry optional expiry time in seconds, 0 (stored indefinitely) if not set
   * @tparam T The type of the object
   * @return The resulting document after it has been inserted
   */
  def insertDocument[T](entity: T, key: String, expiry: Int = 0,
                        persist: PersistTo = PersistTo.NONE, replicate: ReplicateTo = ReplicateTo.NONE)
                       (implicit format: JsonSerializer[T]): Future[DocumentResponse[T]] = {
    val jsonString = format.serialize(entity)
    val doc = RawJsonDocument.create(key, expiry, jsonString)
    
    val delay = Delay.exponential(TimeUnit.MILLISECONDS, CouchbaseStreamsWrapper.temporaryFailureRetryMs)
    val insertObservable =  bucket.async().insert(doc, persist, replicate)
      .retryWhen(RetryBuilder.anyOf(classOf[TemporaryFailureException])
        .delay(delay)
        .max(CouchbaseStreamsWrapper.temporaryFailureMaxRetries)
        .build())
        
    convertToEntity[T](insertObservable)
  }

  /**
   * Lookup a document in couchbase by the key, and unmarshal it into an object: T
    *
    * @param key The key to lookup in the database
   * @tparam T The type to unmarshal the returned document to
   * @return A Future with the object T if found, otherwise None
   */
  def lookupByKey[T](key: String)(implicit format: JsonSerializer[T]): Future[DocumentResponse[T]] = {
    val docObservable = bucket.async().get(key, classOf[RawJsonDocument])
    convertToEntity[T](docObservable)
  }

  def binaryLookupByKey(key: String): Future[DocumentResponse[ByteString]] = {
    val docObservable = bucket.async().get(key, classOf[BinaryDocument])
    observableToOptionFuture(docObservable).map { docOpt =>
      val doc = docOpt.getOrElse(throw new DocumentNotFound(""))
      val bytes = ByteString(doc.content.nioBuffer())
      ReferenceCountUtil.release(doc.content)
      DocumentResponse(cas = doc.cas(), entity = bytes)
    }
  }

  def binaryInsertDocument(data: ByteString, key: String, expiry: Int = 0): Future[DocumentResponse[ByteString]] = {
    val buffer = Unpooled.copiedBuffer(data.toArray[Byte])
    val doc = BinaryDocument.create(key, expiry, buffer)
    observableToFuture(bucket.async().insert(doc)).map { doc =>
      DocumentResponse(cas = doc.cas(), entity = ByteString())
    }
  }

  /**
   * Remove a document from the database
    *
    * @param key The document to remove
   * @return A Successful future if the document was found, otherwise a Failure
   */
  def removeByKey(key: String): Future[JsonDocument] = {
    observableToOptionFuture(bucket.async().remove(key)).map(_.getOrElse(throw new DocumentNotFound("")))
  }

  /**
   * Extract an entity of type T from an AsyncViewRow
    *
    * @param row The AyncViewRow to extract the entity from
   * @tparam T The type of the entity to unmarshal from the row
   * @return The unmarshalled entity if successful
   */
  def getEntityFromRow[T](row: AsyncViewRow)
                         (implicit format: JsonSerializer[T]):
  Future[DocumentResponse[T]] = {
    convertToEntity[T](row.document(classOf[RawJsonDocument]))
  }

  /**
   * Query the designDoc/viewDoc for the given key, and return the resulting row if found
    *
    * @param designDoc The design document containing the view
   * @param viewDoc The view to query
   * @param key The key to lookup in the query
   * @param forceIndex Set to true to force the view index to update on request. This should be used carefully
   * @return The row containing the key if found
   */
  def indexQuerySingleElement(designDoc: String, viewDoc: String, key: String, forceIndex: Boolean=false):
  Future[Option[AsyncViewRow]] = {
    val staleState = if (forceIndex) Stale.FALSE else Stale.TRUE
    observableToOptionFuture(indexQuery(designDoc, viewDoc, List(key), staleState).take(1))
  }

  /**
   * Query an index for multiple items
    *
    * @param designDoc The design document name to query
   * @param viewDoc The view in that design doc
   * @param keys The list of keys to query for
   * @param stale Allow potentially stale indexes or not
   * @return Observable of AsyncViewRow
   */
  def indexQuery(designDoc: String, viewDoc: String, keys: List[String] = List(), stale: Stale = Stale.FALSE,
                limit: Int = Int.MaxValue, skip: Int = 0): Observable[AsyncViewRow] = {
    val query =
      if (keys.isEmpty)
        ViewQuery.from(designDoc, viewDoc)
          .stale(stale)
          .inclusiveEnd(true)
          .limit(limit)
          .skip(skip)
      else
        ViewQuery.from(designDoc, viewDoc)
          .stale(stale)
          .inclusiveEnd(true)
          .keys(JsonArray.from(keys.asJava))
          .limit(limit)
          .skip(skip)
    bucket.async().query(query).flatMap(queryResult => queryResult.rows())
  }

  /**
   * Query an index with a compound key
    *
    * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param keys A List of lists to query for
   * @param stale Allow stale records
   * @return Observable of AsyncViewRows
   */
  def compoundIndexQuery(designDoc: String, viewDoc: String, keys: Option[List[List[Any]]] = None,
                         startKey: Option[Seq[Any]] = None, endKey: Option[Seq[Any]] = None,
                         stale: Stale = Stale.FALSE, limit: Int = Int.MaxValue, skip: Int = 0): Observable[AsyncViewRow] = {
    // Couchbase needs a java.util.List
    var query = ViewQuery
      .from(designDoc, viewDoc)
      .stale(stale)
      .inclusiveEnd(true)
      .limit(limit)
      .skip(skip)

    if (keys.nonEmpty) {
      val keyList: java.util.List[java.util.List[Any]] = keys.get.map(_.asJava).asJava
      query = query.keys(JsonArray.from(keyList))
    }

    if (startKey.nonEmpty) {
      query = query.startKey(JsonArray.from(startKey.get.asJava))
    }

    if (endKey.nonEmpty) {
      query = query.endKey(JsonArray.from(endKey.get.asJava))
    }

    bucket.async().query(query).flatMap(queryResult => queryResult.rows())
  }

  def paginatedIndexQuery[T](designDoc: String, viewDoc: String, startKey: Seq[Any], endKey: Seq[Any],
                          startDocId: Option[String] = None, stale: Stale = Stale.FALSE, limit: Int = 100)
                         (implicit format: JsonSerializer[T]): Future[ViewQueryResponse[T]] = {
    if (startKey.length != endKey.length) throw new IllegalArgumentException("startKey and endKey must be the same length")

    val query = ViewQuery
      .from(designDoc, viewDoc)
      .stale(stale)
      .startKey(JsonArray.from(startKey.asJava))
      .endKey(JsonArray.from(endKey.asJava))
      .startKeyDocId(startDocId.getOrElse(""))
      .limit(limit)
      .skip(if (startDocId.isDefined) 1 else 0)
      .includeDocs(classOf[RawJsonDocument])

    observableToFuture(bucket.async().query(query))
      .map { queryResult =>
        val rows = observableToSource(withDocuments(queryResult.rows()))
          .map(convertToEntity[T](_))
        ViewQueryResponse[T](
          rows = rows,
          errors = observableToOptionFuture(queryResult.error()).map(_.getOrElse(JsonObject.empty())),
          debug = queryResult.debug()
        )
      }
  }

  /**
   * Get the documents found from an Observable[AsyncViewRow]
    *
    * @param docObservable An Observable[AsyncViewRow] as the list of documents
   * @return A new Observable[RawJsonDocument]
   */
  def withDocuments(docObservable: Observable[AsyncViewRow]): Observable[RawJsonDocument] = {
    docObservable.flatMap(_.document(classOf[RawJsonDocument]))
  }


  /**
   * Query an index and unmarshal the found documents into an entity of type T
    *
    * @param designDoc The name of the design document to query
   * @param viewDoc The name of the view
   * @param keys The keys to query for
   * @param stale Allow stale records or not
   * @tparam T The entity type to unmarshal to
   * @return A Source[T, Any] of the found documents.
   */
  def indexQueryToEntity[T](designDoc: String, viewDoc: String, keys: List[String] = List(), stale: Stale = Stale.FALSE,
                           limit: Int = Int.MaxValue, skip: Int = 0)
                           (implicit format: JsonSerializer[T]): Source[DocumentResponse[T], Any] = {
    val query = indexQuery(designDoc, viewDoc, keys, stale, limit, skip)
    val docs = withDocuments(query)
    Source.fromPublisher(RxReactiveStreams.toPublisher(docs)).map(convertToEntity[T])
  }

  /**
   * Query a compound index by a set of keys and unmarshal the found documents into an entity of type T
   *
   * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param keys The list of keys to query for, where each compound key is a list of keys
   * @param stale Allow stale records or not
   * @tparam T The type of the entity to unmarshal to
   * @return A Source[T, Any] of the found documents.
   */
  def compoundIndexQueryByKeysToEntity[T](designDoc: String, viewDoc: String, keys: Option[List[List[Any]]] = None,
                                    stale: Stale = Stale.FALSE, limit: Int = Int.MaxValue, skip: Int = 0)
                                   (implicit format: JsonSerializer[T]):
  Source[DocumentResponse[T], Any] = {
    val query = compoundIndexQuery(designDoc, viewDoc, keys, None, None, stale, limit, skip)
    val docs = withDocuments(query)
    Source.fromPublisher(RxReactiveStreams.toPublisher(docs)).map(convertToEntity[T])
  }

  /**
   * Query a compound index with a start & end range and unmarshal the results
   * @param designDoc The name of the design document
   * @param viewDoc The name of the view
   * @param startKey compound start key range, optional
   * @param endKey compound end key range, optional
   * @param stale Allow stale records or not
   * @param limit number of results to check, defaults to Int.Max
   * @param skip number of results to skip, defaults to 0
   * @tparam T type of entity to unmarshall to
   * @return A Source[T, Any] of the found documents.
   */
  def compoundIndexQueryByRangeToEntity[T](designDoc: String, viewDoc: String, startKey: Option[Seq[Any]] = None,
                                           endKey: Option[Seq[Any]] = None, stale: Stale = Stale.FALSE,
                                           limit: Int = Int.MaxValue, skip: Int = 0)
                                          (implicit format: JsonSerializer[T]):
  Source[DocumentResponse[T], Any] = {
    val query = compoundIndexQuery(designDoc, viewDoc, None, startKey, endKey, stale, limit, skip)
    val docs = withDocuments(query)
    Source.fromPublisher(RxReactiveStreams.toPublisher(docs)).map(convertToEntity[T])
  }

  /**
    * Query couchbase using the N1QL interface. There must be an index created on the bucket for this to succeed.
    *
    * Comment: This is a leaky abstraction around the Couchbase N1QL interface, but it didn't seem worth it to
    *          attempt to re-create their query DSL. An alternative is to just accept strings for queries, but
    *          that doesn't seem like a better solution either.
    *
    * @param q The query statement to execute. You *MUST* select meta().cas
    * @param params Optional query parameters for N1QL. Most important is the .adhoc() option, which should always be set to
    *               false for production queries which are executed frequently. This is the default behavior.
    *               Also important is the ScanConsistency option (similar to the Stale option on view queries).
    * @return A QueryResponse with the rows, as well as info, status, and errors from the query.
    */
  def n1qlQuery(q: Statement, params: N1qlParams = N1qlParams.build().adhoc(false)): Future[RawQueryResponse] = {
    val query = N1qlQuery.simple(q, params)
    observableToFuture(bucket.async().query(query)).map { queryResult =>
      RawQueryResponse(
        observableToSource[AsyncN1qlQueryRow](queryResult.rows()),
        status = observableToFuture[String](queryResult.status()),
        errors = observableToOptionFuture[JsonObject](queryResult.errors()).map(_.getOrElse(JsonObject.empty())),
        info = observableToFuture[N1qlMetrics](queryResult.info())
      )
    }
  }

  /**
    * Query couchbase using the N1QL interface, and convert the found rows into Case Classes of type T.
    * There must be an index created on this bucket to succeed. This interface always uses select(*), since
    * otherwise it is not possible to convert the resulting rows into T (since they will be missing fields).
    *
    * Comment: This is a leaky abstraction around the Couchbase N1QL interface, but it didn't seem worth it to
    *          attempt to re-create their query DSL. An alternative is to just accept strings for queries, but
    *          that doesn't seem like a better solution either.
    *
    * @param where The expressing selecting which records to return.
    * @param order The order to return the records, and the field to sort by
    * @param params Optional query parameters for N1QL. Most important is the .adhoc() option, which should always be
    *               set to false for production queries which are executed frequently. This is the default behavior.
    * @param limit The maximum number of rows to return
    * @param offset The number of rows to skip
    * @param format The JSON protocol to convert the found rows into case classes.
    * @tparam T The case class type to convert to.
    * @return A QueryResponse with the rows, as well as info, status, and errors from the query.
    */
  def n1qlQueryToEntity[T](where: Expression, order: Sort, params: N1qlParams = N1qlParams.build().adhoc(false),
                           limit: Int = 100, offset: Int = 0)
                          (implicit format: JsonSerializer[T]): Future[QueryResponse[T]] = {
    val s: Statement = select("*, meta().cas")
      // Need to wrap bucket name with i() because of the - in the name
      .from(i(bucketName))
      .where(where)
      .orderBy(order)
      .limit(limit)
      .offset(offset)
    val q = N1qlQuery.simple(s, params)
    observableToFuture(bucket.async().query(q)).map { queryResult =>
      QueryResponse[T](
        rows = observableToSource[AsyncN1qlQueryRow](queryResult.rows())
          .map{ row =>
            val cas = row.value().getLong("cas")
            val entity = format.parse(row.value().getObject(bucketName).toString)
            DocumentResponse[T](cas, entity)
          },
        status = observableToFuture[String](queryResult.status()),
        errors = observableToFuture[JsonObject](queryResult.errors()),
        info = observableToFuture[N1qlMetrics](queryResult.info())
      )
    }
  }

  /**
    * Query couchbase with a parameterized query
    *
    * @param query The parameterized query to run
    * @return A RawQueryResponse
    */
  def n1qlParameterizedQuery(query: ParameterizedN1qlQuery): Future[RawQueryResponse] = {
    observableToFuture(bucket.async().query(query))
      .map { queryResult =>
        RawQueryResponse(
          observableToSource[AsyncN1qlQueryRow](queryResult.rows()),
          status = observableToFuture[String](queryResult.status()),
          errors = observableToOptionFuture[JsonObject](queryResult.errors()).map(_.getOrElse(JsonObject.empty())),
          info = observableToFuture[N1qlMetrics](queryResult.info()),
        )
      }
  }

  /**
    * Query couchbase with a parameterized query, and extract entities from the results. You must use
    * select * for this to work, or the entity will be missing fields.
    *
    * @param query The parameterized query to run
    * @tparam T The entity type
    * @return A QueryResponse with the results.
    */
  def n1qlParameterizedQueryToEntity[T](query: ParameterizedN1qlQuery)
                                       (implicit format: JsonSerializer[T]): Future[QueryResponse[T]] = {
    observableToFuture(bucket.async().query(query)).map { queryResult =>
      QueryResponse[T](
        rows = observableToSource[AsyncN1qlQueryRow](queryResult.rows())
          .map { row =>
            val cas = row.value().getLong("cas")
            val entity = format.parse(row.value().getObject(bucketName).toString)
            DocumentResponse[T](cas, entity)
          },
          status = observableToFuture[String](queryResult.status()),
          errors = observableToOptionFuture[JsonObject](queryResult.errors()).map(_.getOrElse(JsonObject.empty())),
          info = observableToFuture[N1qlMetrics](queryResult.info())
      )
    }
  }
}
