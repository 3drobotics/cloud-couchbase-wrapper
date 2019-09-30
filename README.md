Couchbase Scala
===============

This is a wrapper for Akka Streams around the RxJava Couchbase client

TODO
====
Looking for a better way to return the CAS value inside the library

Test setup
==========

```
docker run --rm --detach -p 8091-8094:8091-8094 -p 11210:11210 --name couchbase couchbase:latest
docker exec couchbase /opt/couchbase/bin/couchbase-cli cluster-init --cluster-username Administrator --cluster-password password --cluster-ramsize 300 --cluster-name test --services data,index,query
docker exec couchbase /opt/couchbase/bin/couchbase-cli bucket-create -c localhost -u Administrator -p password --bucket cloud-couchbase-wrapper-test --bucket-type couchbase --bucket-ramsize=100 --enable-flush=1 --wait
sbt test
```

Publishing
==========
To push a new version, run
```
sbt publish
```

Or, if you want to test your changes locally:
```
sbt publishLocal
```