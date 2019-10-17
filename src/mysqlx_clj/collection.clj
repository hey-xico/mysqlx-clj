(ns mysqlx-clj.collection
  (:require [cheshire.core :refer :all])
  (:import (com.mysql.cj.xdevapi JsonParser AddStatementImpl Collection)))

(defn find-by-id
  "Returns a single document with matching _id field."
  [^Collection collection id]
  (if-let [document (-> (.find collection "_id = :_id")
                        (.bind "_id" id)
                        .execute
                        .fetchOne)]
    (parse-string (str document) true)))

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