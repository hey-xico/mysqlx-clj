(ns mysqlx-clj.query
  (:import (com.mysql.cj.xdevapi FindStatementImpl)))

(def logical-query-operators {:and "AND"
                              :or  "OR"})

(def comparison-query-operators {:eq   "="
                                 :like "LIKE"
                                 :gt   ">"
                                 :gte  ">="
                                 :lt   "<"
                                 :lte  "<="})
(defn- hash-string
  [value]
  (str (Math/abs (hash value))))

(defn search-condition-struct
  [condition comparison-operators]
  (let [struct (partial format "%s %s :%s")
        c-operator (first (keys condition))
        operator-value (get comparison-operators c-operator)
        field-name (-> condition c-operator :field)
        field-value (-> condition c-operator :value)]
    (when-not operator-value
      (throw (ex-info (str "Illegal Comparison Operator: " c-operator)
                      {:message (str "Available operators: " comparison-operators)})))
    (struct field-name
            operator-value
            (hash-string field-value))))

(defn assembly-search-condition
  [logical-operation conditions]
  (loop [condition conditions
         search-condition ""]
    (if (empty? condition)
      search-condition
      (let [[f & remaining] condition]
        (recur remaining
               (str search-condition
                    (search-condition-struct f comparison-query-operators)
                    (when (seq remaining)
                      (str " " (get logical-query-operators logical-operation) " "))))))))

(defn bind-search-condition
  [find-statement conditions]
  (reduce (fn [fnd-stm condition]
            (let [operation (first (keys condition))]
              (.bind ^FindStatementImpl fnd-stm
                     (hash-string (-> condition operation :value))
                     (-> condition operation :value))))
          find-statement
          conditions))
