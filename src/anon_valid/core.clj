(ns anon-valid.core
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.java.jdbc :as sql] 
            [clojure.tools.logging :as log] 
            [jdbc.pool.c3p0 :as pool] 
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [clojure.term.colors :refer :all]
            [anon-valid.db :as db]
            [anon-valid.cache :as c]
            [anon-valid.field-handler :as fh]))

(defn- full-table-name [table-def]
  (let [{:keys [table_schem table_name]} table-def]
    (if table_schem (str table_schem "." table_name) table_name)))

; Two special progress method for debugging
(defn- nil-progress [stage & args] nil)
(defn- debug-progress [ & args] (prn args))

(defn- no-cache[& params]
  "This is a default cache which ignores cache setting and returns no value for cache requests"
  false)

(defn- cached-row-counter
  "creates a row counting function with cache handler"
  [cache-fn cache-key res-fn]
  (fn[table]
    (sql/with-db-connection [con db/pool] 
      (if-let [res (cache-fn (cache-key table))]
        (assoc table :cached true :row_count (res 2))
        (let [table (assoc table 
                           :row_count 
                           (biginteger (:result (first (sql/query con (db/qb*row-count (full-table-name table)))))))]
          (cache-fn (cache-key table) (res-fn table))
          table)))))

(defn- row-counted-tables
  [cache-fn]
  (let [tables (db/get-tables)
        cache-key (fn[table] [:table-row-count (full-table-name table)])
        res-fn (fn[table] (reduce #(conj %1 (table %2)) [] [:table_schem :table_name :row_count]))]
    (map (cached-row-counter cache-fn cache-key res-fn) tables)))

(defn non-empty-tables 
  [cache-fn]
  (filter #(> (:row_count %) 0) (row-counted-tables cache-fn)))

(defn table-row-counts 
  "Counts row numbers in tables.
  progress-fn is called at the following stages:
  * :start - at the beginning of listing
  * :row-count schema table row-count - the number of rows in the table. Only called for non-empty tables.
  * :cached-row-count schema table row-count - for cached results
  * :end hash-map(:scanned-tables :non-empty-tables) at the end of listing"
  ([cache-fn progress-fn]
   (sql/with-db-connection [con db/pool] 
                           (table-row-counts con cache-fn progress-fn)))
  ([] (table-row-counts no-cache nil-progress))
  ([con cache-fn progress-fn]
    (let [res-fn (fn[table] (reduce #(conj %1 (table %2)) [] [:table_schem :table_name :row_count]))]
      (progress-fn :start)
      (doall 
        (map #(apply progress-fn 
                     (if (% :cached) :cached-row-count :row-count)
                     (res-fn %)) non-empty-tables))
      (progress-fn :end
                   (hash-map
                     :scanned-tables (count (row-counted-tables cache-fn)) 
                     :non-empty-tables (count non-empty-tables))))))

(defn sensitive-fields 
  ([] (sql/with-db-connection [con db/pool] (doall (sensitive-fields con))))
  ([con]
    (let [fields (transduce (comp (db/fs*min-length 5) (db/fs*sensitive)) conj (db/get-fields con))]
      (reduce #(assoc %1 (:table_name %2) (conj (%1 (:table_name %2)) (:column_name %2))) {} fields))))
  
(defn sensitive-fields-alternative
  ([] (sql/with-db-connection [con db/pool] (doall (sensitive-fields-alternative con))))
  ([con]
    (let [fields (transduce (comp (db/fs*min-length 5) (db/fs*sensitive)) conj (db/get-fields con))]
      (into {} (map #(vector (key %) (map :column_name (val %))) (group-by #(:table_name %) fields))))))

(defn sample-field [con table field]
  (filter #(or (integer? %) (not(empty? %))) 
          (map :result (sql/query con (db/qb*sample-field table field)))))

(defn sample-fields [con table fields]
  (let [sample (map #(vector % (sample-field con table %)) fields)
        non-empty-sample (filter #(> (count (% 1)) 0) sample)]
    (if (not(empty? non-empty-sample)) (vector table non-empty-sample) ())))

(defn sample-sensitive-field-names []
  (log/info "Sampling sensitive fields")
  (sql/with-db-connection [con db/pool]
    (let [fields (sensitive-fields con)
          field-values (map #(sample-fields con (key %) (val %)) fields)
          field-values (filter #(not(empty? %)) field-values)]
      (pp/pprint field-values))))

(defn map-fields-to-values 
  "Maps a list of fields to possible sensitive values. If data-name is given only sensitive values from that 
  data name is used."
  ([data-name fields]
   (let [values-for-field (fn [result-so-far field-def] 
     (if (db/stringish? (:type_name field-def))
       (if-let [filtered (fh/s-data*filter-values-by-max data-name (:column_size field-def))] 
         (conj result-so-far (conj filtered (:column_name field-def)))
         result-so-far)
       result-so-far))]
    (remove nil? (reduce values-for-field () (list fields)))))
  ([fields]
   (map-fields-to-values nil fields)))
                                    
(defn pair-fields-with-data-names
  "Creates a seq of (field data-name) pairs suitable for data matching."
  [fields]
  (let [pair-fn (fn [result-so-far field-def] 
                  (if (db/stringish? (:type_name field-def))
                    (if-let [filtered (fh/s-data*data-names-by-max (:column_size field-def))] 
                      (apply conj result-so-far (map vector (repeat field-def) filtered))
                      result-so-far)
                    result-so-far))] 
    (remove nil? (reduce pair-fn () (seq fields)))))
  
(defn- contains-sensitive-value? 
  [con table-def]
  (let [fields (db/get-fields con (:table_name table-def))
        fields-with-values (map-fields-to-values fields)]
    (if-not (empty? fields-with-values)
      (let [query-string (db/qb*verify-table-contains-sensitive-data (db/exact-table-name table-def) fields-with-values)
;;                  xxx (println query-string)
            result (sql/query con query-string)]
        (> (count result) 0))
      false)))

(defn mark-tables-sensitivity
  ([con cache-fn progress-fn]
   (progress-fn :start (db/table-count))
   (let [tables (db/get-tables con) 
         ;;         tables (filter #(re-find #"JOBS" (:table_name %))  tables)
         cache-key (fn[result] (let [{:keys [table_schem table_name]} result]
                     [:table-sensitivity table_schem table_name]))
         sensitivity-marker (fn[table] 
                              (if-let [res (cache-fn (cache-key table))]
                                (do 
                                  (progress-fn :cached-table-sensitivity 
                                               (:table_schem table)
                                               (:table_name table) (first res))
                                  (assoc table :sensitive? (first res)))
                                (do 
                                  (let [res (contains-sensitive-value? con table)]
                                    (cache-fn (cache-key table) (list res))
                                    (progress-fn :table-sensitivity 
                                                 (:table_schem table)
                                                 (:table_name table) res)
                                    (assoc table :sensitive? res)))))]
         (map sensitivity-marker tables)))
  ([cache-fn progress-fn]
   (sql/with-db-connection [con db/pool]
                           (doall (mark-tables-sensitivity con cache-fn progress-fn))))
  ([] (mark-tables-sensitivity no-cache nil-progress)))

(defn scan-one-for-fields-with-sensitive-values
  "Scans one table for sensitive fields"
  [con progress-fn table-def] 
  (let [fields (db/get-fields con (:table_name table-def))
        flds-data-name (pair-fields-with-data-names fields)
        cached-field (fn[[field data-name]] 
                       (if (nil? (c/field-sensitive-to-data-name? field data-name))
                         true
                         false))
        flds-data-name (filter cached-field flds-data-name)
        field-scan (fn[field data-name] 
                      (let [field-with-values (map-fields-to-values data-name (list field))
                            query-string (db/qb*verify-table-contains-sensitive-data 
                                           (db/exact-table-name table-def) 
                                           field-with-values)]
                        (if-not (empty? field-with-values) 
                          (sql/query con query-string))))
        map-fn (fn[[field data-name :as fld-data-name]]
                 (let [result (field-scan field data-name)
                       result-fn (fn [x] (->> 
                                           x
                                           (map (keyword (string/lower-case (:column_name field)))) 
                                           distinct 
                                           (map #(str "'" % "'"))))]
                   (if (> (count result) 0)
                     (do (progress-fn 
                           :sensitive-field 
                           (db/exact-table-name field) 
                           (:column_name field) data-name (result-fn result)) 
                       fld-data-name))))]
  (doall (remove nil? (map map-fn flds-data-name)))))

(defn scan-one-field-for-sensitive-value
  "Scans one field for sensitive data, returns *resutl-set-limit* number of sample."
  [field sensitive-data] 
  (sql/with-db-connection [con db/pool]
    (let [field-with-values (map-fields-to-values sensitive-data field)
          extract-field (fn [x] (->> 
            x
            (map (keyword (string/lower-case (:column_name field)))) 
            distinct 
            (map #(str "'" % "'"))))
          query-string (db/qb*verify-table-contains-sensitive-data 
                         (db/exact-table-name field) 
                         field-with-values)
          result (extract-field (sql/query con query-string))]
      result)))

(defn list-sensitive-field-candidates
  "Dumps field definitions of tables from the given schema.
  progress-fn is called at the following stages of the execution:
  * :start - at the beginning of the exectuion.
  * :cached-sensitive-field-candidate field [sensitive-data*] - for each cached result.
  * :sensitive-field-candidate field sensitive-data - sensitive-data is a list of possible sensitve-data types.
  * :end - at the end of the execution."
  ([cache-fn progress-fn table-list]
   (sql/with-db-connection [con db/pool] 
     (let [cache-key (fn[table] (let [{:keys [table_schem table_name]} table]
                                   (:sensitive-field-candidate table_schem table_name)))
           process-fn (fn[table]
                        (if-let [res (cache-fn (cache-key table))] 
                          (doall (map #(apply progress-fn :cached-sensitive-field-candidate %) res))
                          (let [field-data-name-pairs (pair-fields-with-data-names (db/get-fields (table :table_name)))]
                            (cache-fn (cache-key table) field-data-name-pairs)
                            (doall (map #(apply progress-fn :sensitive-field-candidate %) field-data-name-pairs)))))]
       (progress-fn :start)
       (doall (map process-fn table-list))
       (progress-fn :end))))
  ([] (list-sensitive-field-candidates no-cache nil-progress (non-empty-tables no-cache)))
  ([progress-fn] (list-sensitive-field-candidates no-cache progress-fn (non-empty-tables no-cache)))
  ([cache-fn progress-fn] (list-sensitive-field-candidates cache-fn progress-fn (non-empty-tables cache-fn))))

(defn scan-fields-with-sensitive-values-x
  "Scans database for table fields with sensitive values.
  progress-fn is called at the following stages of the scan:
  * :start table-count - at the beginning of scan returning the number of tables.
  * :sensitive-table table-name - the table contains sensitive data
  * :not-sensitive-table table-name - the table doesn't contain sensitive data
  * :sensitive-field table-name field-name data-name examples - table's field contains values from data name (plus some examples)"
  ([cache-fn progress-fn table-finder-fn]
   (sql/with-db-connection [con db/pool]
     (doall (map #(scan-one-for-fields-with-sensitive-values con progress-fn %) (table-finder-fn con cache-fn progress-fn)))))
  ([cache-fn progress-or-table-def]
   (if (fn? progress-or-table-def)
     (scan-fields-with-sensitive-values-x cache-fn progress-or-table-def mark-tables-sensitivity)
     (scan-fields-with-sensitive-values-x cache-fn nil-progress (fn[& args] progress-or-table-def))))
  ([progress-or-table-def] 
   (scan-fields-with-sensitive-values-x no-cache progress-or-table-def)))

(defn scan-fields-with-sensitive-values
  "Scans database for table fields with sensitive values.
  progress-fn is called at the following stages of the scan:
  * :start table-count - at the beginning of scan returning the number of tables.
  * :sensitive-field field data-name sample - table's field contains values from data name (plus some examples)"
  ([cache-fn progress-fn table-list]
   (let [field-scanner (fn[stage & args]
                         (if (.contains (str stage) "sensitive")
                           (let [field (first args)
                                 sensitive-data (second args)
                                 cache-key (fn[field data-name] (let [{:keys [table_schem table_name column_name]} field]
                                            [:one-field-scanner-result table_schem table_name column_name data-name]))]
                             (if-let [[sensitive sample] (cache-fn (cache-key field sensitive-data))]
                               (do
                                 (if sensitive (progress-fn :cached-sensitive-field field sensitive-data sample))
                                 sample)
                               (let [sample (scan-one-field-for-sensitive-value field sensitive-data)]
                                   (cache-fn (cache-key field sensitive-data) [(not(empty? sample)) sample])
                                   (if-not (empty? sample) (progress-fn :sensitive-field field sensitive-data sample))
                                   sample)))))]
     (doall (list-sensitive-field-candidates cache-fn field-scanner table-list))))
  ([cache-fn progress-or-table-def]
   (cond
     (fn? progress-or-table-def) (scan-fields-with-sensitive-values cache-fn progress-or-table-def (non-empty-tables cache-fn)) 
     (string? progress-or-table-def) (scan-fields-with-sensitive-values cache-fn nil-progress (list progress-or-table-def))
     (seq? progress-or-table-def) (scan-fields-with-sensitive-values cache-fn nil-progress progress-or-table-def)))
  ([progress-or-table-def] 
   (scan-fields-with-sensitive-values no-cache progress-or-table-def)))

(defn dump-table-definitions
  "Dumps table names from the given schema.
  progress-fn is called at the following stages of the execution:
  * :start :dtd - at the beginning of the exectuion.
  * :cached-table-definition table-structure - for each cached table definition
  * :table-definition table-structure - for each table definition (if no cache is used every call is from this type)
  * :end - at the end of the execution."
  ([cache-fn progress-fn]
   (sql/with-db-connection [con db/pool]
     (let [tables (db/get-tables con)
           cache-key (fn[result] (let [{:keys [table_schem table_type table_name]} result]
                                   [:table-definition table_schem table_type table_name]))
           process-fn (fn[table]
                        (if-let [res (cache-fn (cache-key table))] 
                          (progress-fn :cached-table-definition res)
                          (do (cache-fn (cache-key table) table)
                            (progress-fn :table-definition table))))]
       (progress-fn :start :dtd)
       (doall (map process-fn tables))
       (progress-fn :end :dtd))))
  ([progress-fn] (dump-table-definitions no-cache progress-fn)))

(defn dump-field-definitions
  "Dumps field definitions of tables from the given schema.
  progress-fn is called at the following stages of the execution:
  * :start - at the beginning of the exectuion.
  * :cached-field-definition field-definition - for each cached field definition
  * :field-definition field-structure - for each field definition (if no cache is used every call is from this type)
  * :end - at the end of the execution."
  ([cache-fn progress-fn]
   (sql/with-db-connection [con db/pool]
     (let [tables (db/get-tables con)
           cache-key (fn[result] (let [{:keys [table_schem table_name]} result]
                       [:table-fields table_schem table_name]))
           process-fn (fn[table]
                        (progress-fn :table-start table)
                        (if-let [res (cache-fn (cache-key table))] 
                          (progress-fn :cached-table-field-definition res)
                          (sql/with-db-connection [con2 db/pool]
                            (let [fields (doall (db/get-fields con2 (:table_name table)))] 
                              (cache-fn (cache-key table) fields)
                              (progress-fn :table-field-definition fields)))))]
       (progress-fn :start :dfd)
       (doall (map process-fn tables))
       (progress-fn :end :dfd))))
  ([progress-fn] (dump-table-definitions no-cache progress-fn)))

