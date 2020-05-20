Couchbase Scala
===============

This is a wrapper for Akka Streams around the RxJava Couchbase client

If you're using N1QL exclusively, also consider [Alpakka Couchbase](https://doc.akka.io/docs/alpakka/current/couchbase.html). Unlike Alpakka, this library also wraps map-reduce views and provides automatic serialization/deserialization with Play-JSON.

Test setup
==========

```
docker run --rm --detach -p 8091-8094:8091-8094 -p 11210:11210 --name couchbase couchbase:latest
docker exec couchbase /opt/couchbase/bin/couchbase-cli cluster-init --cluster-username Administrator --cluster-password password --cluster-ramsize 300 --cluster-name test --services data,index,query
docker exec couchbase /opt/couchbase/bin/couchbase-cli bucket-create -c localhost -u Administrator -p password --bucket cloud-couchbase-wrapper-test --bucket-type couchbase --bucket-ramsize=100 --enable-flush=1 --wait
sbt test
```
