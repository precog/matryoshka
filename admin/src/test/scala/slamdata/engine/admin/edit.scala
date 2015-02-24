package slamdata.engine.admin

import org.specs2.mutable._

class EditDialogSpecs extends Specification {
  import MountEditDialog._

  "uriRegex" should {
    "parse simple URI" in {
      "mongodb://localhost" match {
        case UriPattern(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse simple URI with trailing slash" in {
      "mongodb://localhost/" match {
        case UriPattern(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse user/password" in {
      "mongodb://me:pwd@localhost" match {
        case UriPattern(user, pwd, _, _, _, _, _) => user must_== "me"; pwd must_== "pwd"
      }
    }

    "parse port" in {
      "mongodb://localhost:5555" match {
        case UriPattern(_, _, _, port, _, _, _) => port.toInt must_== 5555
      }
    }

    "parse database" in {
      "mongodb://localhost/test" match {
        case UriPattern(_, _, _, _, _, db, _) => db must_== "test"
      }
    }

    "parse additional host(s)" in {
      "mongodb://host1,host2,host3:5555" match {
        case UriPattern(_, _, _, _, hosts, _, _) => hosts must_== ",host2,host3:5555"
      }
    }

    "parse options" in {
      "mongodb://host/?option1=foo&option2=bar" match {
        case UriPattern(_, _, _, _, _, _, options) => options must_== "option1=foo&option2=bar"
      }
    }

    "parse all of the above" in {
      "mongodb://me:pwd@host1:5555,host2:5559,host3/test?option1=foo&option2=bar" match {
        case UriPattern(user, pwd, host, port, extraHosts, db, options) =>
          user must_== "me"
          pwd must_== "pwd"
          host must_== "host1"
          port.toInt must_== 5555
          extraHosts must_== ",host2:5559,host3"
          db must_== "test"
          options must_== "option1=foo&option2=bar"
      }
    }
  }
}
