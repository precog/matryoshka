package quasar

import java.io.File

import Predef._
import org.specs2.time.NoTimeConversions
import quasar.Backend.{FilesystemNode}
import quasar.fs.Path
import quasar.specs2.DisjunctionMatchers


class InteractiveTest extends BackendTest with NoTimeConversions with DisjunctionMatchers {

  backendShould { (prefix, _, backend, name) =>

    def assertNotThere(file: Path) = {
      val listings = interactive.ls(backend, prefix).run
      listings must not contain (FilesystemNode(file, None))
    }

    def assertThere(file: Path) = {
      val listings = interactive.ls(backend, prefix).run
      listings must contain(FilesystemNode(file, None))
    }

    // NB: this test has nothing interesting to say about read-only
    // connections at present.
    if (name != TestConfig.MONGO_READ_ONLY)
      "Interactive" should {
        "load test data correctly" in {
          interactive.withTemp(backend, prefix) { tempFile =>
            interactive.delete(backend, prefix ++ tempFile).run
            assertNotThere(tempFile)
            interactive.loadData(backend, prefix ++ tempFile, interactive.zips.run.content).run.run
            assertThere(tempFile)
          }
        }
        "load file correctly" in {
          val collName = "zips"
          val path = prefix ++ Path(collName)
          val file = new File(s"it/src/main/resources/tests/$collName.data")
          interactive.delete(backend, path).run
          assertNotThere(path)
          interactive.loadFile(backend,prefix, file).run.run
          assertThere(Path(collName))
        }
      }
    ()
  }
}
