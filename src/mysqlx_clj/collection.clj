(ns mysqlx-clj.collection
  (:require [cheshire.core :refer :all])
  (:import (com.mysql.cj.xdevapi JsonParser AddStatementImpl Collection)))

(defn- do-find [statement]
  (if-let [document (-> statement
                        .execute
                        .fetchOne)]
    (parse-string (str document) true)))

(defn find-by-id
  "Returns a single document with matching _id field."
  [^Collection collection id]
  (do-find (-> (.find collection "_id = :_id")
               (.bind "_id" id))))

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
  (let [search-conditions (str (-> query :eq :field) " = :" (-> query :eq :field))
        find-stm (-> (.find collection search-conditions)
                     (.bind (-> query :eq :field) (-> query :eq :value)))]
    (do-find find-stm)))


