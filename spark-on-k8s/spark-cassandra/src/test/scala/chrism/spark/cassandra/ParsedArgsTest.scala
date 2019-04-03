package chrism.spark.cassandra

import org.junit.runner.RunWith
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ParsedArgsTest extends FunSuite with TypeCheckedTripleEquals {

  test("None is returned when -h or --help is specified") {
    assert(ParsedArgs.parse(Array("-h")).isEmpty)
    assert(ParsedArgs.parse(Array("--help")).isEmpty)
  }

  test("can parse arguments when only required arguments are specified") {
    val requiredArgs = Array("--operation", "read", "--cassandra-host", "hostname", "--keyspace", "ks", "--table", "t")

    val parsed = ParsedArgs.parse(requiredArgs).getOrElse(fail(s"Failed to parse ${requiredArgs.mkString(" ")}"))
    assert(parsed === ParsedArgs("hostname", "ks", "t", Read))
  }

  test("missing required arguments results in IllegalArgumentException") {
    // missing --operation
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--cassandra-host", "hostname", "--keyspace", "ks", "--table", "t"))
    }

    // missing --cassandra-host
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--operation", "read", "--keyspace", "ks", "--table", "t"))
    }

    // missing --keyspace
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--operation", "read", "--cassandra-host", "hostname", "--table", "t"))
    }

    // missing --table
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--operation", "read", "--cassandra-host", "hostname", "--keyspace", "ks"))
    }
  }
}
