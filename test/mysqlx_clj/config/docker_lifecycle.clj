(ns mysqlx_clj.config.docker-lifecycle
  (:import (org.testcontainers.containers GenericContainer)
           (org.testcontainers.containers.wait.strategy Wait)))

(def connection-properties
  {"MYSQL_DATABASE"      "mysqlx-clj"
   "MYSQL_USER"          "clj"
   "MYSQL_PASSWORD"      "s3cr3t"
   "MYSQL_ROOT_PASSWORD" "root"
   "TZ"                  "UTC"})

(defn- container []
  (let [image-tag "mysql:8.0.13"
        x-port (into-array Integer [(Integer. 33060)])
        startup-signal (Wait/forLogMessage ".*X Plugin ready for connections.*\\s" 2)
        environment connection-properties]
    (-> (GenericContainer. image-tag)
        (.withEnv environment)
        (.withExposedPorts x-port)
        (.waitingFor startup-signal))))

(defn initialize []
  (let [mysql-8-container (container)]
    (.start mysql-8-container)
    mysql-8-container))

(defn running? [container]
  (if container
    (.isRunning container)))

(defn host-address [container]
  (if container
    (.getIpAddress container)
    "localhost"))

(defn host-port [container]
  (if container
    (.getMappedPort container 33060)
    30000))

(defn destroy [container]
  (.stop container))
