(ns com.chicoalmeida.mysqlx-clj.collection-test
  (:require [clojure.test :refer :all]
            [clojure.string :refer [split]]
            [com.chicoalmeida.mysqlx-clj.config.docker-lifecycle :as docker]
            [com.chicoalmeida.mysqlx-clj.core :as core]
            [com.chicoalmeida.mysqlx-clj.collection :as target])
  (:import (java.util UUID)
           (clojure.lang ExceptionInfo)
           (com.mysql.cj.exceptions WrongArgumentException)))
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

(def star-wars-fxt [{:name       "Luke Skywalker",
                     :height     "172",
                     :mass       "77",
                     :hair_color "blond",
                     :skin_color "fair",
                     :eye_color  "blue",
                     :birth_year "19BBY",
                     :gender     "male"}
                    {:name       "C-3PO",
                     :height     "167",
                     :mass       "75",
                     :hair_color "n/a",
                     :skin_color "gold",
                     :eye_color  "yellow",
                     :birth_year "112BBY",
                     :gender     "n/a"}
                    {:name       "R2-D2",
                     :height     "96",
                     :mass       "32",
                     :hair_color "n/a",
                     :skin_color "white, blue",
                     :eye_color  "red",
                     :birth_year "33BBY",
                     :gender     "n/a"}
                    {:name       "Darth Vader",
                     :height     "202",
                     :mass       "136",
                     :hair_color "none",
                     :skin_color "white",
                     :eye_color  "yellow",
                     :birth_year "41.9BBY",
                     :gender     "male"}
                    {:mass       "49",
                     :birth_year "19BBY",
                     :skin_color "light",
                     :name       "Leia Organa",
                     :hair_color "brown",
                     :gender     "female",
                     :eye_color  "brown",
                     :homeworld  "https//swapi.co/api/planets/2/",
                     :height     "150"}
                    {:name       "Owen Lars",
                     :height     "178",
                     :mass       "120",
                     :hair_color "brown, grey",
                     :skin_color "light",
                     :eye_color  "blue",
                     :birth_year "52BBY",
                     :gender     "male"}
                    {:name       "Beru Whitesun lars",
                     :height     "165",
                     :mass       "75",
                     :hair_color "brown",
                     :skin_color "light",
                     :eye_color  "blue",
                     :birth_year "47BBY",
                     :gender     "female"}
                    {:name       "R5-D4",
                     :height     "97",
                     :mass       "32",
                     :hair_color "n/a",
                     :skin_color "white, red",
                     :eye_color  "red",
                     :birth_year "unknown",
                     :gender     "n/a"}
                    {:name       "Biggs Darklighter",
                     :height     "183",
                     :mass       "84",
                     :hair_color "black",
                     :skin_color "light",
                     :eye_color  "brown",
                     :birth_year "24BBY",
                     :gender     "male"}
                    {:name       "Obi-Wan Kenobi",
                     :height     "182",
                     :mass       "77",
                     :hair_color "auburn, white",
                     :skin_color "fair",
                     :eye_color  "blue-gray",
                     :birth_year "57BBY",
                     :gender     "male"}])
;
; TESTS
;


(deftest insert-data
  (testing "given open session, valid collection name and document should insert and return result"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
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
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
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
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
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
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))
          ;and
          result (target/find-by-id collection (random-string))]

      (is (nil? result))
      ;after
      (.dropCollection schema (.getName collection)))))

(deftest find-statements
  (testing "finding by single field"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
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
      (is (= 1
             (count result)))
      (is (= (:name document-fxt)
             (:name (first result))))
      ;after
      (.dropCollection schema (.getName collection))))
  (testing "finding by multiple fields"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))

          ;and
          id-fxt (random-string)
          name-fxt (random-string)
          document-fxt {:_id  id-fxt
                        :name name-fxt}
          query-fxt {:and [{:eq {:field "_id"
                                 :value id-fxt}}
                           {:eq {:field "name"
                                 :value name-fxt}}]}
          ;and
          _ (target/insert! collection document-fxt)

          ;when
          result (target/find collection query-fxt)]
      (is (not (nil? result)))
      (is (= (:name document-fxt)
             (:name (first result))))
      ;after
      (.dropCollection schema (.getName collection))))

  (testing "quering with like"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))

          ;and
          _ (target/insert! collection star-wars-fxt)

          ;when
          leia-and-luke (target/find collection {:and [{:like {:field "birth_year"
                                                               :value "19%"}}]})]
      (is (not (nil? leia-and-luke)))
      (is (= 2
             (count leia-and-luke)))
      ;after
      (.dropCollection schema (.getName collection))))
  (testing "quering with gt"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))

          ;and
          _ (target/insert! collection star-wars-fxt)

          ;when
          result (target/find collection {:gt {:field "mass"
                                               :value "75"}})]
      (is (not (nil? result)))
      (is (= 3
             (count result)))
      ;after
      (.dropCollection schema (.getName collection))))

  (testing "quering with greater than and equals to"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props)) collection (.createCollection schema (random-string))

          ;and
          _ (target/insert! collection star-wars-fxt)

          ;when
          result (target/find collection {:gte {:field "mass"
                                                :value "75"}})]
      (is (not (nil? result)))
      (is (= 5
             (count result)))
      ;after
      (.dropCollection schema (.getName collection))))

  (testing "quering with between using gt and lt"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))

          ;and
          _ (target/insert! collection star-wars-fxt)

          ;when
          result (target/find collection {:and [{:gt {:field "mass"
                                                      :value "75"}}
                                                {:lt {:field "mass"
                                                      :value "84"}}]})]
      (is (not (nil? result)))
      (is (= 2
             (count result)))
      ;after
      (.dropCollection schema (.getName collection))))
  (testing "quering with between using gte and lte"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))
          ;and
          _ (target/insert! collection star-wars-fxt)

          ;when
          result (target/find collection {:and [{:gte {:field "mass"
                                                       :value "75"}}
                                                {:lte {:field "mass"
                                                       :value "84"}}]})]

      (is (not (nil? result)))
      (is (= 5
             (count result)))
      ;after
      (.dropCollection schema (.getName collection))))
  (testing "quering with invalid logical structure"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection (.createCollection schema (random-string))
          ;and
          _ (target/insert! collection star-wars-fxt)]

      (is (thrown-with-msg? ExceptionInfo #"Illegal query construction" (target/find collection {:and {:gte {:field "mass"
                                                                                                             :value "75"}}})))
      ;after
      (.dropCollection schema (.getName collection)))))

(deftest retrieve-collection
  (testing "given valid schema and an existing collection retrieve it"
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection-name (random-string)
          collection (.createCollection schema collection-name)
          ;and
          result (target/get-collection schema collection-name)]

      (is (not (nil? result)))
      ;after
      (.dropCollection schema collection-name))
    )
  (testing "given valid schema and a collection that not exists fail "
    (let [;given
          connection-props {:host     (docker/host-address @container)
                            :port     (docker/host-port @container)
                            :dbname   (docker/connection-properties "MYSQL_DATABASE")
                            :user     (docker/connection-properties "MYSQL_USER")
                            :password (docker/connection-properties "MYSQL_PASSWORD")}
          session (core/open-session connection-props)
          schema (core/get-schema session (:dbname connection-props))
          collection-name (random-string)]

      (is (thrown-with-msg? WrongArgumentException #" doesn't exist" (target/get-collection schema collection-name)))
      (.dropCollection schema collection-name))
    ))