(ns anon-valid.core
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] 
         '[clojure.tools.logging :as log] 
         '[jdbc.pool.c3p0 :as pool] 
         '[clojure.pprint :as pp]
         '[anon-valid.db :as db]
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as fh])

(defn- full-table-name [table-def]
  (let [{:keys [table_schem table_name]} table-def]
    (if table_schem (str table_schem "." table_name) table_name)))

(defn table-row-counts []
  (log/info "Printing table row counts")
  (sql/with-db-connection [con db/pool]
    (let [tables (db/get-tables con)
          data-tables (map #(assoc % :row_count
                                   (biginteger (:result (first (sql/query con (db/qb*row-count (full-table-name %1))))))) 
                           tables)
          non-zero-tables (filter #(> (:row_count %) 0) data-tables)] 
      (doall 
        (map #(println (:table_name %1) (:row_count %1)) non-zero-tables))
      (printf "Number of non-empty tables: %d\n" (count non-zero-tables)))))

(defn sensitive-fields 
  ([] (sql/with-db-connection [con db/pool] (doall (sensitive-fields con))))
  ([con]
    (let [fields (transduce (comp (db/fs*length 5) (db/fs*sensitive)) conj (db/get-fields con))]
      (reduce #(assoc %1 (:table_name %2) (conj (%1 (:table_name %2)) (:column_name %2))) {} fields))))
  
(defn sensitive-fields-alternative
  ([] (sql/with-db-connection [con db/pool] (doall (sensitive-fields-alternative con))))
  ([con]
    (let [fields (transduce (comp (db/fs*length 5) (db/fs*sensitive)) conj (db/get-fields con))]
      (into {} (map #(vector (key %) (map :column_name (val %))) (group-by #(:table_name %) fields))))))

(defn print-sensitive-fields []
  (log/info "Printing sensitive fields")
  (sql/with-db-connection [con db/pool]
    (let [fields (sensitive-fields con)]
      (doall 
        (map #(println (format "%s: %d: %s" %1 (count (fields %1)) (pr-str (fields %1)))) (keys fields))))))

(defn sample-field [con table field]
  (filter #(or (integer? %) (not(empty? %))) 
          (map :result (sql/query con (db/qb*sample-field table field)))))

(defn sample-fields [con table fields]
  (let [sample (map #(vector % (sample-field con table %)) fields)
        non-empty-sample (filter #(> (count (% 1)) 0) sample)]
    (if (not(empty? non-empty-sample)) (vector table non-empty-sample) ())))

(defn sample-sensitive-fields []
  (log/info "Sampling sensitive fields")
  (sql/with-db-connection [con db/pool]
    (let [fields (sensitive-fields con)
          field-values (map #(sample-fields con (key %) (val %)) fields)
          field-values (filter #(not(empty? %)) field-values)]
      (pp/pprint field-values))))

(defn map-table-fields-to-values [fields]
  (let [values-for-field (fn [result-so-far field-def] 
                           (if (db/stringish? (:type_name field-def))
                             (if-let [filtered (fh/s-data*filter-max (:column_size field-def))] 
                               (conj result-so-far (conj filtered (:column_name field-def))))))] 
    (remove nil? (reduce values-for-field () fields))))

(defn- contains-sensitive-value? 
  [con table-def]
  (let [fields (db/get-fields con (:table_name table-def))
        fields-with-values (map-table-fields-to-values (db/get-fields con (:table_name table-def)))]
    (if-not (empty? fields-with-values)
      (let [query-string (db/qb*verify-table-contains-sensitive-data (db/exact-table-name table-def) fields-with-values)
            ;;      xxx (println query-string)
            result (sql/query con query-string)]
        (> (count result) 0)))))

(defn- nil-progress [stage & args] nil)

(defn tables-with-sensitive-values 
  ([progress-fn]
   (progress-fn :start)
   (sql/with-db-connection [con db/pool]
     (let [tables (db/get-tables con) 
           ;;          tables (filter #(= (:table_name %) "CUSTOMERS") tables)
           table-filter (fn[table-def] 
                          (if (contains-sensitive-value? con table-def)
                            (do (progress-fn :sensitive table-def) true)
                            (do (progress-fn :not-sensitive table-def) false)))
           result (filter table-filter tables)]
       (doall result))))
  ([]
   (tables-with-sensitive-values nil-progress)))

(defn fields-with-sensitive-values
  ([progress-fn table-def] 
   )
  ([progress-fn table-def & tables]
   (sql/with-db-connection [con db/pool]
   ))
  ([table-def]
   (fields-with-sensitive-values nil-progress table-def)))



