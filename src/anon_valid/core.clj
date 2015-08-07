(ns anon-valid.core
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] 
         '[clojure.tools.logging :as log] 
         '[jdbc.pool.c3p0 :as pool] 
         '[anon-valid.db :as db]
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as field-handler])

;; field filters
(defn ff*stringish
  []
  (filter #(not (nil? (db/stringish-field-types (:type_name %1)))))) 

(defmacro ff*min-length
  [len]
  `(filter #(> (:column_size %1) ~len)))

(defn table-row-counts []
  (log/info "Printing table row counts")
  (sql/with-db-connection [con db/pool]
    (let [tables (db/get-tables con)
          data-tables (map #(assoc % :row_count
                                   (:result (first (sql/query con [(db/qb*row-count (:table_name %1))])))) tables)
          non-zero-tables (filter #(> (:row_count %) 0) data-tables)] 
      (doall 
        (map #(println (:table_name %1) (:row_count %1)) non-zero-tables))
      (printf "Number of non-empty tables: %d\n" (count non-zero-tables)))))

(defn sensitive-fields []
  (log/info "Printing sensitive fields")
  (sql/with-db-connection [con db/pool]
    (let [fields (transduce (comp (db/fs*length 5) (db/fs*sensitive)) conj (db/get-fields con))
          by-tables (reduce #(assoc %1 (:table_name %2) (conj (%1 (:table_name %2)) (:column_name %2))) {} fields)]
      (doall 
        (map #(println (format "%s: %d: %s" %1 (count (by-tables %1)) (by-tables %1))) (keys by-tables))))))
  
