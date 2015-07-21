(ns anon-valid.main
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.tools.logging :as log] 
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as fh]
         '[anon-valid.db :as db])

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (log/info "Hello, World!")
  (fh/load-definitions)
  (log/info (db/tables-with-sensitive-fields))
  ;(println (sql/query connect-db ["select * from jos_vm_module"]))
  ;(log/info (core/get-tables))
  ;(log/info (map :column_name (core/get-fields "jos_vm_product")))
  )
