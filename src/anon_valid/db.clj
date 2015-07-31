(ns anon-valid.db
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] 
         '[clojure.tools.logging :as log] 
         '[jdbc.pool.c3p0 :as pool] 
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as fh])

(def db-spec {:subprotocol "mysql"
              :subname "//localhost:3306/xxx"
              :user "test"
              :password "test"})

(def pool
  (pool/make-datasource-spec db-spec))

(def stringish-field-types #{"VARCHAR" "CHAR" "TEXT"})

(defn get-tables 
  ([] (sql/with-db-connection [con pool] (get-tables con)))
  ([con] (doall (resultset-seq (.getTables (.getMetaData (:connection con)) nil nil nil (into-array ["TABLE" "VIEW"]))))))

(defn get-fields 
  ([table] (sql/with-db-connection [con pool] (doall (resultset-seq (.getColumns (.getMetaData (:connection con)) nil nil table nil)))))
  ([] (get-fields nil)))

(defn table-name []
  (map :table_name))

;; {:table_cat "xxx", :table_schem nil, :table_name "jos_vm_product", :column_name "product_id", :data_type 4, :type_name "INT", :column_size 10, :buffer_length 65535, :decimal_digits 0, :num_prec_radix 10, :nullable 0, :remarks "", :column_def nil, :sql_data_type 0, :sql_datetime_sub 0, :char_octet_length nil, :ordinal_position 1, :is_nullable "NO", :scope_catalog nil, :scope_schema nil, :scope_table nil, :source_data_type nil, :is_autoincrement "YES"}

(defn field-name [] 
  (map :column_name))

;; query builder
(defmacro qb*row-count [table-name] 
  `(str "select count(*) as result from " ~table-name))
