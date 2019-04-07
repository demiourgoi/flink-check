package es.ucm.fdi.sscheck.flink

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import org.apache.flink.api.scala._
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.matcher.ResultMatchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.{Failure, Try}


@RunWith(classOf[JUnitRunner])
class ConcurrentActionsTest  extends Specification with ResultMatchers {
  def is =
    s2"""
        - where two sequential calls to collect work ok $doubleCollectSequential
        - where two concurrent calls to collect break Flink's OperatorTranslation.translateToPlan,
      and with a race condition ${doubleCollectConcurrent}
        - where flink-check matchers triggered from a parallel collection also break Flink's
        OperatorTranslation.translateToPlan, and with a race condition ${flinkCheckMatchersParCollection}
       - where flink-check matchers triggered from a sequential collection work ok ${flinkCheckMatchersSeqCollection}
      """

  // TODO: make parallelism of Formula configuravble, and force relaible failure with  Formula => new test
  // TODO: allow disable paraleloism in formaula, and see it works => new test

  val logger = LoggerFactory.getLogger(getClass)

  def logFailure[A](testName: String)(body: => A): Try[A] =
    Try { body } recoverWith{ case t =>
      logger.error(testName, t)
      Failure(t)
    }

  def fixture = new {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    val xs = env.fromCollection(1 to 100).map{_+1}
  }

  def doubleCollectSequential = {
    val f = fixture

    println(s"xs.count = ${f.xs.count}")
    println(s"xs.count = ${f.xs.count}")
    ok
  }

  /*
Diverse exceptions, non deterministically generated depending on race conditions

java.lang.NullPointerException
	at org.apache.flink.api.java.operators.OperatorTranslation.translate(OperatorTranslation.java:63)
	at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:52)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
	at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
	at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
	at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
	at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply$mcV$sp(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)

java.lang.NullPointerException
	at org.apache.flink.api.common.JobExecutionResult.getAccumulatorResult(JobExecutionResult.java:90)
	at org.apache.flink.api.scala.DataSet.count(DataSet.scala:720)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply$mcV$sp(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)
* */
  def doubleCollectConcurrent = {
    val f = fixture
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

    logFailure("doubleCollectConcurrent"){
      val pendingActions = Seq.fill(10)(
        Future { println(s"xs.count = ${f.xs.count}") }
      )
      val pendingActionsFinished = Future.fold(pendingActions)(Unit){ (u1, u2) =>
        println("pending action finished")
        Unit
      }
      Await.result(pendingActionsFinished, 10 seconds)
    } should beFailedTry
  }

  /*
 java.util.ConcurrentModificationException
at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:51)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:17)
at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:14)
at es.ucm.fdi.sscheck.flink.ConcurrentActionsTest$$anonfun$concurrentFlinkMatchers$1.apply(ConcurrentActionsTest.scala:65)
at es.ucm.fdi.sscheck.flink.ConcurrentActionsTest$$anonfun$concurrentFlinkMatchers$1.apply(ConcurrentActionsTest.scala:64)

/usr/lib/jvm/java-1.8.0-openjdk/bin/java -ea -Didea.test.cyclic.buffer.size=1048576 -javaagent:/opt/intellij/idea-IC-183.5429.30/lib/idea_rt.jar=32965:/opt/intellij/idea-IC-183.5429.30/bin -Dfile.encoding=UTF-8 -classpath /opt/intellij/idea-IC-183.5429.30/lib/idea_rt.jar:/opt/intellij/idea-IC-183.5429.30/plugins/junit/lib/junit-rt.jar:/opt/intellij/idea-IC-183.5429.30/plugins/junit/lib/junit5-rt.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/charsets.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/cldrdata.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/dnsns.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/jaccess.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/localedata.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/nashorn.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/sunec.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/ext/zipfs.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/jce.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/jsse.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/management-agent.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/resources.jar:/usr/lib/jvm/java-1.8.0-openjdk/jre/lib/rt.jar:/home/juanrh/git/investigacion/demiourgoi/flink-check/flink-check/target/scala-2.11/test-classes:/home/juanrh/git/investigacion/demiourgoi/flink-check/flink-check/target/scala-2.11/classes:/home/juanrh/.ivy2/cache/org.specs2/specs2-scalacheck_2.11/jars/specs2-scalacheck_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.scala-lang.modules/scala-java8-compat_2.11/bundles/scala-java8-compat_2.11-0.7.0.jar:/home/juanrh/.ivy2/cache/com.typesafe.akka/akka-slf4j_2.11/jars/akka-slf4j_2.11-2.4.20.jar:/home/juanrh/.ivy2/cache/com.typesafe.akka/akka-actor_2.11/jars/akka-actor_2.11-2.4.20.jar:/home/juanrh/.ivy2/cache/com.typesafe/config/bundles/config-1.3.0.jar:/home/juanrh/.ivy2/cache/xmlenc/xmlenc/jars/xmlenc-0.52.jar:/home/juanrh/.ivy2/cache/xml-apis/xml-apis/jars/xml-apis-1.3.04.jar:/home/juanrh/.ivy2/cache/xerces/xercesImpl/jars/xercesImpl-2.9.1.jar:/home/juanrh/.ivy2/cache/org.sonatype.sisu.inject/cglib/jars/cglib-2.2.1-v20090111.jar:/home/juanrh/.ivy2/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.7.10.jar:/home/juanrh/.ivy2/cache/org.mortbay.jetty/jetty-util/jars/jetty-util-6.1.26.jar:/home/juanrh/.ivy2/cache/org.mortbay.jetty/jetty-sslengine/jars/jetty-sslengine-6.1.26.jar:/home/juanrh/.ivy2/cache/org.fusesource.leveldbjni/leveldbjni-all/bundles/leveldbjni-all-1.8.jar:/home/juanrh/.ivy2/cache/org.codehaus.jettison/jettison/bundles/jettison-1.1.jar:/home/juanrh/.ivy2/cache/org.codehaus.jackson/jackson-xc/jars/jackson-xc-1.9.13.jar:/home/juanrh/.ivy2/cache/org.codehaus.jackson/jackson-mapper-asl/jars/jackson-mapper-asl-1.9.13.jar:/home/juanrh/.ivy2/cache/org.codehaus.jackson/jackson-jaxrs/jars/jackson-jaxrs-1.9.13.jar:/home/juanrh/.ivy2/cache/org.codehaus.jackson/jackson-core-asl/jars/jackson-core-asl-1.9.13.jar:/home/juanrh/.ivy2/cache/org.apache.zookeeper/zookeeper/jars/zookeeper-3.4.6.jar:/home/juanrh/.ivy2/cache/org.apache.httpcomponents/httpcore/jars/httpcore-4.2.4.jar:/home/juanrh/.ivy2/cache/org.apache.httpcomponents/httpclient/jars/httpclient-4.2.5.jar:/home/juanrh/.ivy2/cache/org.apache.htrace/htrace-core/jars/htrace-core-3.1.0-incubating.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-yarn-server-common/jars/hadoop-yarn-server-common-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-yarn-common/jars/hadoop-yarn-common-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-yarn-client/jars/hadoop-yarn-client-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-yarn-api/jars/hadoop-yarn-api-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-mapreduce-client-shuffle/jars/hadoop-mapreduce-client-shuffle-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-mapreduce-client-jobclient/jars/hadoop-mapreduce-client-jobclient-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-mapreduce-client-core/jars/hadoop-mapreduce-client-core-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-mapreduce-client-common/jars/hadoop-mapreduce-client-common-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-mapreduce-client-app/jars/hadoop-mapreduce-client-app-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-hdfs/jars/hadoop-hdfs-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-common/jars/hadoop-common-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-client/jars/hadoop-client-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-auth/jars/hadoop-auth-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.hadoop/hadoop-annotations/jars/hadoop-annotations-2.7.4.jar:/home/juanrh/.ivy2/cache/org.apache.directory.server/apacheds-kerberos-codec/bundles/apacheds-kerberos-codec-2.0.0-M15.jar:/home/juanrh/.ivy2/cache/org.apache.directory.server/apacheds-i18n/bundles/apacheds-i18n-2.0.0-M15.jar:/home/juanrh/.ivy2/cache/org.apache.directory.api/api-util/bundles/api-util-1.0.0-M20.jar:/home/juanrh/.ivy2/cache/org.apache.directory.api/api-asn1-api/bundles/api-asn1-api-1.0.0-M20.jar:/home/juanrh/.ivy2/cache/org.apache.curator/curator-recipes/bundles/curator-recipes-2.7.1.jar:/home/juanrh/.ivy2/cache/org.apache.curator/curator-framework/bundles/curator-framework-2.7.1.jar:/home/juanrh/.ivy2/cache/org.apache.curator/curator-client/bundles/curator-client-2.7.1.jar:/home/juanrh/.ivy2/cache/org.apache.avro/avro/jars/avro-1.7.4.jar:/home/juanrh/.ivy2/cache/log4j/log4j/bundles/log4j-1.2.17.jar:/home/juanrh/.ivy2/cache/javax.xml.stream/stax-api/jars/stax-api-1.0-2.jar:/home/juanrh/.ivy2/cache/javax.xml.bind/jaxb-api/jars/jaxb-api-2.2.2.jar:/home/juanrh/.ivy2/cache/javax.servlet.jsp/jsp-api/jars/jsp-api-2.1.jar:/home/juanrh/.ivy2/cache/javax.servlet/servlet-api/jars/servlet-api-2.5.jar:/home/juanrh/.ivy2/cache/javax.inject/javax.inject/jars/javax.inject-1.jar:/home/juanrh/.ivy2/cache/javax.activation/activation/jars/activation-1.1.jar:/home/juanrh/.ivy2/cache/io.netty/netty-all/jars/netty-all-4.0.23.Final.jar:/home/juanrh/.ivy2/cache/io.netty/netty/bundles/netty-3.7.0.Final.jar:/home/juanrh/.ivy2/cache/commons-net/commons-net/jars/commons-net-3.1.jar:/home/juanrh/.ivy2/cache/commons-logging/commons-logging/jars/commons-logging-1.1.3.jar:/home/juanrh/.ivy2/cache/commons-lang/commons-lang/jars/commons-lang-2.6.jar:/home/juanrh/.ivy2/cache/commons-httpclient/commons-httpclient/jars/commons-httpclient-3.1.jar:/home/juanrh/.ivy2/cache/commons-digester/commons-digester/jars/commons-digester-1.8.jar:/home/juanrh/.ivy2/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.6.jar:/home/juanrh/.ivy2/cache/commons-codec/commons-codec/jars/commons-codec-1.4.jar:/home/juanrh/.ivy2/cache/commons-beanutils/commons-beanutils-core/jars/commons-beanutils-core-1.8.0.jar:/home/juanrh/.ivy2/cache/commons-beanutils/commons-beanutils/jars/commons-beanutils-1.7.0.jar:/home/juanrh/.ivy2/cache/com.thoughtworks.paranamer/paranamer/jars/paranamer-2.3.jar:/home/juanrh/.ivy2/cache/com.sun.xml.bind/jaxb-impl/jars/jaxb-impl-2.2.3-1.jar:/home/juanrh/.ivy2/cache/com.sun.jersey.contribs/jersey-guice/jars/jersey-guice-1.9.jar:/home/juanrh/.ivy2/cache/com.sun.jersey/jersey-server/bundles/jersey-server-1.9.jar:/home/juanrh/.ivy2/cache/com.sun.jersey/jersey-json/bundles/jersey-json-1.9.jar:/home/juanrh/.ivy2/cache/com.sun.jersey/jersey-core/bundles/jersey-core-1.9.jar:/home/juanrh/.ivy2/cache/com.sun.jersey/jersey-client/bundles/jersey-client-1.9.jar:/home/juanrh/.ivy2/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-2.5.0.jar:/home/juanrh/.ivy2/cache/com.google.inject/guice/jars/guice-3.0.jar:/home/juanrh/.ivy2/cache/com.google.guava/guava/jars/guava-11.0.2.jar:/home/juanrh/.ivy2/cache/com.google.code.gson/gson/jars/gson-2.2.4.jar:/home/juanrh/.ivy2/cache/com.google.code.findbugs/jsr305/jars/jsr305-3.0.0.jar:/home/juanrh/.ivy2/cache/asm/asm/jars/asm-3.1.jar:/home/juanrh/.ivy2/cache/aopalliance/aopalliance/jars/aopalliance-1.0.jar:/home/juanrh/.ivy2/cache/org.apache.commons/commons-lang3/jars/commons-lang3-3.3.2.jar:/home/juanrh/.ivy2/local/es.ucm.fdi/sscheck-core_2.11/0.3.2/jars/sscheck-core_2.11.jar:/home/juanrh/.ivy2/cache/commons-io/commons-io/jars/commons-io-2.4.jar:/home/juanrh/.ivy2/cache/com.esotericsoftware.minlog/minlog/jars/minlog-1.2.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-matcher_2.11/jars/specs2-matcher_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-matcher-extra_2.11/jars/specs2-matcher-extra_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-junit_2.11/jars/specs2-junit_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-core_2.11/jars/specs2-core_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-common_2.11/jars/specs2-common_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-codata_2.11/jars/specs2-codata_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.specs2/specs2-analysis_2.11/jars/specs2-analysis_2.11-3.8.4.jar:/home/juanrh/.ivy2/cache/org.scalaz/scalaz-effect_2.11/bundles/scalaz-effect_2.11-7.2.3.jar:/home/juanrh/.ivy2/cache/org.scalaz/scalaz-core_2.11/bundles/scalaz-core_2.11-7.2.26.jar:/home/juanrh/.ivy2/cache/org.scalaz/scalaz-concurrent_2.11/bundles/scalaz-concurrent_2.11-7.2.3.jar:/home/juanrh/.ivy2/cache/org.scalacheck/scalacheck_2.11/jars/scalacheck_2.11-1.13.4.jar:/home/juanrh/.ivy2/cache/org.scala-lang.modules/scala-xml_2.11/bundles/scala-xml_2.11-1.0.5.jar:/home/juanrh/.ivy2/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.4.jar:/home/juanrh/.ivy2/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.11.8.jar:/home/juanrh/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.11.8.jar:/home/juanrh/.ivy2/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.11.8.jar:/home/juanrh/.ivy2/cache/org.specs2/classycle/jars/classycle-1.4.3.jar:/home/juanrh/.ivy2/cache/org.scala-sbt/test-interface/jars/test-interface-1.0.jar:/home/juanrh/.ivy2/cache/org.hamcrest/hamcrest-core/jars/hamcrest-core-1.3.jar:/home/juanrh/.ivy2/cache/junit/junit/jars/junit-4.12.jar:/home/juanrh/.ivy2/cache/com.esotericsoftware.kryo/kryo/bundles/kryo-2.24.0.jar:/home/juanrh/.ivy2/cache/com.github.scopt/scopt_2.11/jars/scopt_2.11-3.5.0.jar:/home/juanrh/.ivy2/cache/com.twitter/chill-java/jars/chill-java-0.7.6.jar:/home/juanrh/.ivy2/cache/com.twitter/chill_2.11/jars/chill_2.11-0.7.6.jar:/home/juanrh/.ivy2/cache/com.typesafe/ssl-config-core_2.11/bundles/ssl-config-core_2.11-0.2.1.jar:/home/juanrh/.ivy2/cache/com.typesafe.akka/akka-protobuf_2.11/jars/akka-protobuf_2.11-2.4.20.jar:/home/juanrh/.ivy2/cache/com.typesafe.akka/akka-stream_2.11/jars/akka-stream_2.11-2.4.20.jar:/home/juanrh/.ivy2/cache/commons-cli/commons-cli/jars/commons-cli-1.3.1.jar:/home/juanrh/.ivy2/cache/commons-collections/commons-collections/jars/commons-collections-3.2.2.jar:/home/juanrh/.ivy2/cache/org.apache.commons/commons-compress/jars/commons-compress-1.18.jar:/home/juanrh/.ivy2/cache/org.apache.commons/commons-math3/jars/commons-math3-3.5.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-annotations/jars/flink-annotations-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-clients_2.11/jars/flink-clients_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-core/jars/flink-core-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-hadoop-fs/jars/flink-hadoop-fs-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-java/jars/flink-java-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-metrics-core/jars/flink-metrics-core-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-optimizer_2.11/jars/flink-optimizer_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-queryable-state-client-java_2.11/jars/flink-queryable-state-client-java_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-runtime-web_2.11/jars/flink-runtime-web_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-runtime_2.11/jars/flink-runtime_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-scala_2.11/jars/flink-scala_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-shaded-asm/jars/flink-shaded-asm-5.0.4-5.0.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-shaded-asm-6/jars/flink-shaded-asm-6-6.2.1-5.0.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-shaded-guava/jars/flink-shaded-guava-18.0-5.0.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-shaded-jackson/jars/flink-shaded-jackson-2.7.9-5.0.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-shaded-netty/jars/flink-shaded-netty-4.1.24.Final-5.0.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-streaming-java_2.11/jars/flink-streaming-java_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-streaming-scala_2.11/jars/flink-streaming-scala_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/force-shading/jars/force-shading-1.7.2.jar:/home/juanrh/.ivy2/cache/org.clapper/grizzled-slf4j_2.11/jars/grizzled-slf4j_2.11-1.3.2.jar:/home/juanrh/.ivy2/cache/org.javassist/javassist/bundles/javassist-3.19.0-GA.jar:/home/juanrh/.ivy2/cache/org.objenesis/objenesis/jars/objenesis-2.1.jar:/home/juanrh/.ivy2/cache/org.reactivestreams/reactive-streams/jars/reactive-streams-1.0.0.jar:/home/juanrh/.ivy2/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.1.4.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-connector-filesystem_2.11/jars/flink-connector-filesystem_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.apache.flink/flink-hadoop-compatibility_2.11/jars/flink-hadoop-compatibility_2.11-1.7.2.jar:/home/juanrh/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.15.jar com.intellij.rt.execution.junit.JUnitStarter -ideVersion5 -junit4 es.ucm.fdi.sscheck.flink.ConcurrentActionsTest
19/04/06 17:01:39 ERROR ConcurrentActionsTest: concurrentFlinkMatchers
java.lang.NullPointerException
at org.apache.flink.api.java.operators.OperatorTranslation.translate(OperatorTranslation.java:63)
at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:52)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:17)
 * */
  def flinkCheckMatchersParCollection = {
    val f = fixture
    import scala.collection.parallel._
    val counts = (1 to 10).par
    counts.tasksupport =
      new ThreadPoolTaskSupport(Executors.newFixedThreadPool(10).asInstanceOf[ThreadPoolExecutor])

    logFailure("concurrentFlinkMatchers"){
      counts.foreach{_ =>
        f.xs should foreachElement{_ > 0}
      }
    } should beFailedTry
  }

  def flinkCheckMatchersSeqCollection = {
    val f = fixture
    val counts = (1 to 10)

    counts.foreach{_ =>
      f.xs should foreachElement{_ > 0}
    }

    ok
  }
}

