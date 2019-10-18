(ns mysqlx-clj.collection
  (:require [cheshire.core :refer :all]
            [mysqlx-clj.query :as q])
  (:import (com.mysql.cj.xdevapi JsonParser AddStatementImpl Collection)))

(defn- do-find [statement]
  (if-let [document (-> statement
                        .execute
                        .fetchAll)]
    (parse-string (str document) true)))

(defn find-by-id
  "Returns a single document with matching _id field."
  [^Collection collection id]
  (first (do-find (.bind (.find collection "_id = :_id") "_id" id))))

(defn insert!
  "Saves document to collection and returns the inserted document as a persistent Clojure map."
  [^Collection collection document]
  (if (map? document)
    (let [ids (->> (generate-string document)
                   JsonParser/parseDoc
                   ^AddStatementImpl (.add collection)
                   .execute
                   .getGeneratedIds)]
      (find-by-id collection (first ids)))
    (reduce #(conj %1 (insert! collection %2)) [] document)))

(defn find [collection query]
  (do-find  (if (some (set (keys q/logical-query-operators)) (keys query))
             (let [operation (first (keys query))
                   conditions (get query operation)
                   search-conditions (q/assembly-search-condition operation conditions)]
               (q/bind-search-condition (.find collection search-conditions) conditions))
             (let [search-conditions (q/assembly-search-condition nil [query])]
               (q/bind-search-condition (.find collection search-conditions) [query])))))

