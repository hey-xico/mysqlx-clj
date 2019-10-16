(ns mysqlx-clj.core-test
  (:require [clojure.test :refer :all]
            [bond.james :as bond :refer [with-spy]]
            [mysqlx-clj.core :as target]
            [mysqlx_clj.config.docker-lifecycle :as docker])
  (:import (clojure.lang ExceptionInfo)
           (java.util UUID)))

(def container (atom nil))
(defn random-string []
  (first
    (shuffle
      (clojure.string/split
        (.toString (UUID/randomUUID)) #"-"))))

;(use-fixtures :once (fn [f]
;                      (reset! container (docker/initialize))
;                      (f)
;                      (docker/destroy @container)
;                      ))
(deftest connection-validations
  (testing "given valid credentials should start a valid session"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          ;when
          session (target/open-session connection-props)
          ]
      ;then
      (is (= true
             (target/open? session)))
      )
    )
  (testing "given open session ensure it gets closed"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (target/open-session connection-props)
          ;when
          _ (target/close-session session)
          ]
      ;then
      (is (= false
             (target/open? session)))
      )
    ))
(deftest schema-validations
  (testing "given a valid session create schema should succeed"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :user     "root"
                            :password "root"}
          session (target/open-session connection-props)
          schema-name-fxt (random-string)
          ;when
          result (target/create-schema session schema-name-fxt)]

      (is (= (.getName result)
             schema-name-fxt))
      )
    )
  (testing "attempt to create schema without permission must fail"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (target/open-session connection-props)
          schema-name-fxt (random-string)]


      (is (thrown-with-msg? ExceptionInfo #"Access denied for user" (target/create-schema session schema-name-fxt)))
      ))
  (testing "attempt to create duplicated schema must fail"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :user     "root"
                            :password "root"}
          session (target/open-session connection-props)
          schema-name-fxt (random-string)
          _ (target/create-schema session schema-name-fxt)]


      (is (thrown-with-msg? ExceptionInfo #"^.*database exists*$" (target/create-schema session schema-name-fxt)))
      ))

  (testing "given a valid schema name to drop should succeed"
    (with-spy [target/get-schema-names target/drop-schema]
              (let [;given
                    connection-props {:host     (docker/host-address @container)
                                      :port     (docker/host-port @container)
                                      :user     "root"
                                      :password "root"}
                    session (target/open-session connection-props)
                    schema-name-fxt (random-string)
                    _ (target/create-schema session schema-name-fxt)
                    ]
                ;when
                (target/drop-schema session schema-name-fxt)

                ;then
                (is (= 1 (-> target/get-schema-names bond/calls count)))
                (is (= 1 (-> target/drop-schema bond/calls count)))

                (is (not (contains? (target/get-schema-names session)
                                    schema-name-fxt)))

                )))

  (testing "given a schema that doesn't exists should not call drop"

    (with-spy [target/get-schema-names target/drop-schema]
              (let [;given
                    connection-props {:host     (docker/host-address @container)
                                      :port     (docker/host-port @container)
                                      :user     "root"
                                      :password "root"}
                    session (target/open-session connection-props)]
                ;when
                (target/drop-schema session (random-string))

                ;then
                (is (= 1 (-> target/get-schema-names bond/calls count)))
                (is (= 1 (-> target/drop-schema bond/calls count)))
                )))
  )