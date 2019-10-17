(ns mysqlx-clj.collection-test
  (:require [clojure.test :refer :all]
            [clojure.string :refer [split]]
            [mysqlx-clj.config.docker-lifecycle :as docker]
            [mysqlx-clj.core :as core]
            [mysqlx-clj.collection :as target])
  (:import (java.util UUID)))
;
; SETUP
;

(def container (atom nil))
(defn random-string []
  (first
    (shuffle
      (split (.toString (UUID/randomUUID)) #"-"))))

(use-fixtures :once (fn [f]
                      (if (= (System/getenv "RUNNING_LOCAL") "true")
                        (do (println "you must have docker-compose up and running when this flag is enabled")
                            (f))
                        (do (reset! container (docker/initialize))
                            (f)
                            (docker/destroy @container)))))
(def connection-props {:host     (docker/host-address @container)
                       :port     (docker/host-port @container)
                       :dbname   (docker/connection-properties "MYSQL_DATABASE")
                       :user     (docker/connection-properties "MYSQL_USER")
                       :password (docker/connection-properties "MYSQL_PASSWORD")})
(defn given-valid-session [connection-props]
  (core/open-session connection-props))

;
; TESTS
;


(deftest insert-data
  (testing "given open session, valid collection name and document should insert and return result"
    (let [;given
          schema (core/get-schema (given-valid-session connection-props) (:dbname connection-props))
          collection (.createCollection schema (random-string))

          ;and
          data-fxt {:name      (random-string)
                    :last-name (random-string)
                    :mail      (str (random-string) "@" (random-string) ".com")}

          ;when
          result (target/insert! collection data-fxt)]

      (is (not (nil? (result :_id))))
      (is (= (:name data-fxt) (:name result)))
      (is (= (:last-name data-fxt) (:last-name result)))
      (is (= (:email data-fxt) (:email result)))

      ;after
      (.dropCollection schema (.getName collection))))

  (testing "given mode than one documents should insert and return all"
    (let [;given
          schema (core/get-schema (given-valid-session connection-props) (:dbname connection-props))

          collection (.createCollection schema (random-string))

          ;and
          data-fxt (reduce #(conj %1 {:name      (random-string)
                                      :last-name (random-string)
                                      :mail      (str (random-string) %2 "@" (random-string) ".com")})
                           [] (take (.length (random-string)) (range)))

          ;when
          result (target/insert! collection data-fxt)]

      (is (= (count data-fxt)
             (count result)))

      (doseq [r result]
        (is (not (nil? (:_id r)))))
      ;after
      (.dropCollection schema (.getName collection)))))

(deftest find-by-id
  (testing "given if of existing document should return document"
    (let [;given
          schema (core/get-schema (given-valid-session connection-props) (:dbname connection-props))

          collection (.createCollection schema (random-string))

          ;and
          data-fxt {:name      (random-string)
                    :last-name (random-string)
                    :mail      (str (random-string) "@" (random-string) ".com")}

          inserted-doc (target/insert! collection data-fxt)
          ;when
          result (target/find-by-id collection (:_id inserted-doc))]
      (is (not (nil? (result :_id))))
      (is (= (:_id inserted-doc) (:_id result)))
      (is (= (:name data-fxt) (:name result)))
      (is (= (:last-name data-fxt) (:last-name result)))
      (is (= (:email data-fxt) (:email result)))

      ;after
      (.dropCollection schema (.getName collection))))

  (testing "given if not present should return nil"
    (let [;given
          schema (core/get-schema (given-valid-session connection-props) (:dbname connection-props))
          collection (.createCollection schema (random-string))
          ;and
          result (target/find-by-id collection (random-string))]

      (is (nil? result))
      ;after
      (.dropCollection schema (.getName collection)))))

(deftest find-statements
  (testing "finding by single field"
    (let [;given
          schema (core/get-schema (given-valid-session connection-props) (:dbname connection-props))
          collection (.createCollection schema (random-string))

          ;and
          id-fxt (random-string)
          document-fxt {:_id  id-fxt
                        :name (random-string)}
          query-fxt {:eq {:field "_id"
                          :value id-fxt}}
          ;and
          _ (target/insert! collection document-fxt)

          ;when
          result (target/find collection query-fxt)]
      (is (not (nil? result)))
      (is (= (:name document-fxt)
             (:name result)))
      ;after
      (.dropCollection schema (.getName collection)))))

