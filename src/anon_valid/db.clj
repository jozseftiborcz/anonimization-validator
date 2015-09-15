(ns anon-valid.db
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] 
         '[clojure.tools.logging :as log] 
         '[jdbc.pool.c3p0 :as pool] 
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as fh])

(defn default-port [db-type]
  (case db-type 
    :mysql 3306
    :oracle 1521
    :mssql 1443))

(defn db-driver [db-type]
  (case db-type 
    :mysql "com.mysql.jdbc.Driver"
    :oracle "oracle.jdbc.driver.OracleDriver"
    :mssql "com.microsoft.jdbc.sqlserver.SQLServerDriver"))

(defn build-connect-string [db-type host port database-name host user pwd]
  (case db-type
    :mysql (format "//%s:%d/%s" host port database-name)
    :oracle (format "thin:@%s:%d/%s" host port database-name)
    :mssql (format "//%s:%p;database=%s;user=%s;password=%s" host port database-name user pwd)))

(def db-spec (ref {}))

(defn read-password []
  (let [console (java.lang.System/console)
        pwd (if console (.readPassword console "password:" nil) 
              (do (print "password:") (flush) (read-line)))]
    (apply str pwd)))

(def oracle? false)

(defn set-connection-arg 
  "This parses the connection parameters"
  [options]
  (let [{:keys [db-type host port database-name user pwd]} options
        pwd (if-not pwd (read-password) pwd)
        port (if-not port (default-port db-type) port)
        classname (db-driver db-type)]
    (def oracle? (= db-type :oracle))
    (dosync (alter db-spec assoc 
                   :subprotocol (name db-type) 
                   :classname classname
                   :user user
                   :password pwd
                   :subname (build-connect-string db-type host port database-name host user pwd)))
    (def pool (pool/make-datasource-spec @db-spec))))

(defn test-orcl-pool []
  (def pool (pool/make-datasource-spec {:subprotocol "oracle"
                                        :user "dbtest"
                                        :password "dbtest"
                                        :subname "thin:@localhost:1521/orcl"})))
(defn test-mysql-pool []
  (def pool (pool/make-datasource-spec {:subprotocol "mysql"
                                        :user "test"
                                        :password "test"
                                        :subname "//localhost:3306/xxx"})))

(defn set-and-test-connection [options]
  (set-connection-arg options)
  (let [{:keys [database-name host user]} options]
    (try 
      (log/info (format "Connecting to database %s on host %s with user %s" database-name host user))
      (log/debug @db-spec)
      (with-open [con (sql/get-connection @db-spec)]
        true)
      (catch ^java.sql.SQLException Exception e (log/error (str "Error connecting to database:" (.getMessage e)))))))

(def stringish-field-types #{"VARCHAR" "CHAR" "TEXT"})

(defn get-tables 
  ([] (sql/with-db-connection [con pool] (doall (get-tables con))))
  ;([con] (doall (resultset-seq (.getTables (.getMetaData (:connection con)) nil nil nil (into-array ["TABLE" "VIEW"]))))))
  ([con] (let [results (resultset-seq (.getTables (.getMetaData (:connection con)) nil nil nil (into-array ["TABLE"])))]
           (filter #(or (not oracle?) (not (some #{(:table_schem %)} '("SYS" "SYSTEM")))) results))))

(defn get-fields 
  ([] (get-fields nil))
  ([con-or-table] 
   (if (or (string? con-or-table) (nil? con-or-table)) 
     (sql/with-db-connection [con pool] (doall (get-fields con con-or-table)))
     (get-fields con-or-table nil)))
  ([con table] (resultset-seq (.getColumns (.getMetaData (:connection con)) nil nil table nil))))

(defn table-name []
  (map :table_name))

;; {:table_cat "xxx", :table_schem nil, :table_name "jos_vm_product", :column_name "product_id", :data_type 4, :type_name "INT", :column_size 10, :buffer_length 65535, :decimal_digits 0, :num_prec_radix 10, :nullable 0, :remarks "", :column_def nil, :sql_data_type 0, :sql_datetime_sub 0, :char_octet_length nil, :ordinal_position 1, :is_nullable "NO", :scope_catalog nil, :scope_schema nil, :scope_table nil, :source_data_type nil, :is_autoincrement "YES"}

(defn field-name [] 
  (map :column_name))

(defn alphanum? [x] (re-matches #"\w+" x))

;; query builder
(defmacro qb*row-count [table-name] 
  `[(str "select count(*) as result from " ~table-name)])

(defmacro qb*non-empty-field-count [table-name field-name]
  `[(str "select count(*) as result from ~table-name where ? is not null and ? != ''") ~field-name ~field-name])

(defn qb*sample-field [table-name field-name]
  {:pre [(alphanum? table-name) (alphanum? field-name)]}
  [(str "select distinct " field-name " as result from " table-name " where ? is not null and ? != '' limit 5") field-name field-name])

(defmacro qb*sensitive-column [table-name fields-with-values]
  [(str "select * from dual;")])

;; field selector
(defmacro fs*length
  [size]
  `(filter #(if (:column_size %1) (> (:column_size %1) ~size) true)))

(defmacro fs*sensitive
  []
  `(filter #(fh/sensitive-field? (:column_name %))))
