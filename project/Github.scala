package github

import sbt._
import Keys._

import org.kohsuke.github._
import scala.collection.JavaConversions._

object GithubPlugin extends Plugin {
  object GithubKeys {
    lazy val repoSlug       = settingKey[String]("The repo slug, e.g. 'slamdata/quasar'")
    lazy val tag            = settingKey[String]("The name of the tag, e.g. v1.2.3")
    lazy val releaseName    = taskKey[String]("The name of the release")
    lazy val commitish      = settingKey[String]("The commitish value from which the tag is created")
    lazy val draft          = settingKey[Boolean]("The draft / final flag")
    lazy val prerelease     = settingKey[Boolean]("The prerelease / release flag")
    lazy val assets         = taskKey[Seq[File]]("The binary assets to upload")
    lazy val githubAuth     = taskKey[GitHub]("Creates a Github based on GITHUB_TOKEN OAuth variable")
    lazy val githubRelease  = taskKey[GHRelease]("Publishes a new Github release")

    lazy val versionFile      = settingKey[String]("The JSON version file, e.g. 'version.json")
    lazy val versionRepo      = settingKey[String]("The repo slug for the JSON version file")
    lazy val githubUpdateVer  = taskKey[String]("Updates the JSON version file in the version repo")
  }

  import GithubKeys._

  private object Travis {
    lazy val BuildNumber = Option(System.getenv("TRAVIS_BUILD_NUMBER"))
    lazy val RepoSlug    = Option(System.getenv("TRAVIS_REPO_SLUG"))
    lazy val Commit      = Option(System.getenv("TRAVIS_COMMIT"))
    lazy val IsTravis    = Option(System.getenv("TRAVIS")).fold(false)(_ => true)
  }

  lazy val githubSettings: Seq[Setting[_]] = Seq(
    repoSlug    := Travis.RepoSlug.fold(organization.value + "/" + normalizedName.value)(identity),
    tag         := "v" + version.value +
                   (if (prerelease.value) Travis.BuildNumber.fold("")("-" + _) else "") +
                   "-" + normalizedName.value,
    releaseName := name.value +
                   (" " + tag.value) +
                   (if (draft.value) " (draft)" else ""),
    commitish   := Travis.Commit.getOrElse(""),
    draft       := false,
    prerelease  := version.value.matches(""".*SNAPSHOT.*"""),
    assets      := Seq((packageBin in Compile).value),

    githubAuth := {
      val log = streams.value.log

      val token = Option(System.getenv("GITHUB_TOKEN")).getOrElse(sys.error("You must define GITHUB_TOKEN"))

      val github = GitHub.connectUsingOAuth(token)

      log.info("Connected using GITHUB_TOKEN")

      github
    },

    githubRelease := {
      val log = streams.value.log

      val github = githubAuth.value

      log.info("repoSlug    = " + repoSlug.value)
      log.info("tag         = " + tag.value)
      log.info("releaseName = " + releaseName.value)
      log.info("draft       = " + draft.value)
      log.info("prerelease  = " + prerelease.value)
      log.info("commitish   = " + commitish.value)

      val release = Option({
        val repo = github.getRepository(repoSlug.value).
                    createRelease(tag.value).
                    name(releaseName.value).
                    draft(draft.value).
                    //body(body). // TODO: Build release notes from fixed issues
                    prerelease(prerelease.value)

        commitish.value match {
          case "" => repo
          case v  => repo.commitish(v)
        }
      }.create).getOrElse(sys.error("Could not create the Github release"))

      log.info("Created Github release: " + release)

      assets.value foreach { asset =>
        val relativePath = asset.relativeTo(baseDirectory.value).getOrElse(asset)
        val mimeType     = Option(java.nio.file.Files.probeContentType(asset.toPath())).getOrElse("application/java-archive")

        log.info("Uploading " + relativePath + " (" + mimeType + ") to release")

        release.uploadAsset(asset, mimeType)
      }

      release
    },

    versionFile := "version.json",

    versionRepo := { repoSlug.value },

    githubUpdateVer := {
      val log = streams.value.log

      val ver  = version.value
      val file = versionFile.value
      val repo = versionRepo.value

      log.info("version       = " + ver)
      log.info("version file  = " + file)
      log.info("version repo  = " + repo)

      val github = githubAuth.value

      val content = github.getRepository(repo).getFileContent(file)

      val json = """{"version": """" + ver + """"}"""

      content.update(json, "Releasing " + ver)

      json
    }
  )
}
