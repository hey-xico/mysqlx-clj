(ns com.chicoalmeida.mysqlx-clj.collection
  (:require [cheshire.core :refer :all]
            [com.chicoalmeida.mysqlx-clj.query :as q])
  (:import (com.mysql.cj.xdevapi JsonParser AddStatementImpl Collection Schema)))

(defn get-collection
  "Returns an existing collection from a valid schema"
  [^Schema schema collection-name]
  (.getCollection schema collection-name true))

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

(defn- get-conditions
  [logical-operator query]
  (if logical-operator
    (let [c (get query (first (keys query)))]
      (when (map? (get query (first (keys query))))
        (throw (ex-info (str "Illegal query construction: " query)
                        {:message (str "When using logical operator, the comparisons must be inside a vector:" q/sample-logical-query)})))
      c)
    [query]))

(defn find [collection query]
  (do-find
    (let [l-operator (when (contains? q/logical-query-operators (first (keys query)))
                       (first (keys query)))
          conditions (get-conditions l-operator query)
          search-conditions (q/assembly-search-condition l-operator conditions)]
      (q/bind-search-condition (.find collection search-conditions) conditions))))