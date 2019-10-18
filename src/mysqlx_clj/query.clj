(ns mysqlx-clj.query)

(def logical-query-operators {:and "AND"
                              :or  "OR"})

(def comparison-query-operators {:eq   "="
                                 :like "LIKE"
                                 :gt   ">"
                                 :gte  ">="
                                 :lt   "<"
                                 :lte  "<="})
(def search-condition-struct
  ^{:doc   "The base structure for create search condition like: '_id = :_id'"
    :added "1.0"}
  (partial format "%s %s :%s "))

(defn hash-string
  [value]
  (str (Math/abs (hash value))))

(defn assembly-search-condition
  [logical-operation conditions]
  (loop [condition conditions
         search-condition ""]
    (if (empty? condition)
      search-condition
      (let [[f & remaining] condition
            comparison-operator (first (keys f))]
        (recur remaining
               (str search-condition
                    (search-condition-struct (-> f comparison-operator :field)
                                             (get comparison-query-operators comparison-operator)
                                             (hash-string (-> f comparison-operator :value)))
                    (when (seq remaining)
                      (get logical-query-operators logical-operation)) " "))))))

(defn bind-search-condition
  [find-statement conditions]
  (reduce (fn [fnd-stm condition]
            (let [operation (first (keys condition))]
              (.bind fnd-stm
                     (hash-string (-> condition operation :value))
                     (-> condition operation :value))))
          find-statement
          conditions))

(defn translate-query [query]
  )

