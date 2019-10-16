(ns mysqlx-clj.core
  (:require [clojure.spec.alpha :as s]
            [clojure.walk :refer [stringify-keys postwalk]])
  (:import (com.mysql.cj.xdevapi SessionFactory Session Schema)
           (java.util Properties)))

(s/def ::host string?)
(s/def ::port number?)
(s/def ::user string?)
(s/def ::password string?)
(s/def ::dbname string?)
(s/def ::connection-props (s/keys :req-un [::host ::port ::user ::password]
                                  :opt-un [::dbname]))

(defn create-props
  ^Properties
  [props]
  (let [props# (Properties.)
        stringified-keys (stringify-keys props)
        f (fn [[k v]] (if (string? v) [k v] [k (str v)]))
        _ (.putAll props# (postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) stringified-keys))]
    props#))

(defn open-session
  "Connects to MySql using x protocol."
  {:arglists '([{:keys [host port user password dbname]}])}
  [props]
  {:pre [(s/valid? ::connection-props props)]}
  (let [session-factory (SessionFactory.)]
    (.getSession session-factory (create-props props))))

(defn open?
  [session]
  (.isOpen session))

(defn get-schema-names
  "Gets a list of all schema names present on the server"
  [^Session session]
  (vec (map #(.getName %) (.getSchemas session))))

(defn ^Schema get-schema
  "Get schema reference by name."
  [^Session session ^String name]
  (.getSchema session name))

(defn drop-schema
  "Drops a schema"
  [^Session session ^String schema]
  (let [schemas (get-schema-names session)]
    (if (some #(= schema %)  schemas)
      (do (println "Calling drop")
          (.dropSchema session schema))
      (println "Not Calling drop"))))

(defn create-schemagcb
  "Creates a schema"
  [^Session session ^String schema-name]
  (try
    (.createSchema session schema-name)
    (catch Exception e
      (throw (ex-info (str "Unable to create schema: " (.getMessage e))
                      {:cause   (.getCause e)
                       :message (.getMessage e)}))))
  )

(defn get-url
  "Get the URL used to create this session."
  [^Session session]
  (.getUri session))

(defn close-session
  "Closes the given x protocol session"
  [^Session session]
  (.close session))

(defn start-transaction
  [^Session session]
  (.startTransaction session))

(defn commit
  [^Session session]
  (.commit session))

(defn rollback
  [^Session session]
  (.rollback session))

(defn transactional-execution
  [^Session session f]
  (try
    (start-transaction session)
    (let [r (f)]
      (commit session)
      r)
    (catch Exception e
      (do
        (println "ERRO: " (.getMessage e))
        (rollback session)
        )
      ))
  )