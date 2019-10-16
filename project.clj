(defproject mysqlx-clj "0.0.1"
  :description "FIXME: write description"
  :url "https://github.com/Chicoalmeida/mysqlx-clj"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/data.json "0.2.6"]
                 [mysql/mysql-connector-java "8.0.18"]]
  :plugins [[lein-cloverage "1.0.13"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]]
  :profiles {:dev {:resource-paths ["test/resources"]
                   :plugins        [[com.jakemccrary/lein-test-refresh "0.24.1"]
                                    [lein-cljfmt "0.6.4"]
                                    [lein-cloverage "1.1.1"]
                                    [test2junit "1.2.7"]
                                    [jonase/eastwood "0.3.5"]
                                    [lein-kibit "0.1.7"]]
                   :dependencies   [[org.clojure/clojure "1.10.1"]
                                    [circleci/bond "0.4.0"]
                                    ; logging
                                    [ch.qos.logback/logback-classic "1.2.3" :exclusions [org.slf4j/slf4j-api]]
                                    [org.slf4j/jul-to-slf4j "1.7.28"]
                                    [org.slf4j/jcl-over-slf4j "1.7.28"]
                                    [org.slf4j/log4j-over-slf4j "1.7.28"]
                                    [org.testcontainers/testcontainers "1.12.2"]]}}
  :deploy-repositories [["releases" :clojars]]
  :aliases {"update-readme-version" ["shell" "sed" "-i" "s/\\\\[mysqlx-clj \"[0-9.]*\"\\\\]/[mysqlx-clj \"${:version}\"]/" "README.md"]}
  :release-tasks [["shell" "git" "diff" "--exit-code"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["changelog" "release"]
                  ["update-readme-version"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["deploy"]
                  ["vcs" "push"]])
