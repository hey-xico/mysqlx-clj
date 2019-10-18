(ns mysqlx-clj.query-test
  (:require [clojure.test :refer :all])
  (:require [mysqlx-clj.query :as target])
  (:import (clojure.lang ExceptionInfo)))

(deftest create-search-condition-structure
  (testing "given map condition convert to string strucutre"

    (let [;given
          map-struct {:eq {:field "foo"
                           :value "bar"}}
          ;when
          result (target/search-condition-struct map-struct target/comparison-query-operators)]

      (is (= (str "foo = :" (str (Math/abs (hash (-> map-struct :eq :value)))))
             result))))
  (testing "given map condition convert to string strucutre"

    (let [;given
          map-struct {:and {:field "foo"
                            :value "bar"}}
          ;when
          ]

      (is (thrown-with-msg? ExceptionInfo #"Illegal Comparison Operator: :and" (target/search-condition-struct map-struct target/comparison-query-operators))))))

(deftest create-full-search-condition
  (testing "without logical operator"
    (let [;given
          map-struct {:eq {:field "foo"
                           :value "bar"}}
          ;when
          result (target/assembly-search-condition nil [map-struct])]

      (is (= (str "foo = :" (str (Math/abs (hash (-> map-struct :eq :value)))))
             result))))
  (testing "with logical operator"
    (let [;given
          map-struct [{:eq {:field "foo"
                            :value "bar"}}
                      {:eq {:field "bar"
                            :value "foo"}}]
          ;when
          result (target/assembly-search-condition :and map-struct)]

      (is (= (str "foo = :" (str (Math/abs (hash "bar")))
                  " AND "
                  "bar = :" (str (Math/abs (hash "foo"))))
             result)))))