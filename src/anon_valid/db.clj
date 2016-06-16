(ns anon-valid.db
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] 
         '[clojure.tools.logging :as log] 
         '[jdbc.pool.c3p0 :as pool] 
         '[clojure.string :as string]
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as fh])

;; global default db-type for the actual db session: mysql, oracle, mssql
(def ^:dynamic *db-type* :oracle)
;; default number of rows returned
(def ^:dynamic *result-set-limit* 5)
;; connection options
(def ^:dynamic *command-options* {})

(defn connection?
  "Returns true if the parameter is a db connection"
  [c]
  (and (map? c) (:connection c)))

(defn default-port 
  "Returns the default database port"
  [db-type]
  (case db-type 
    :mysql 3306
    :oracle 1521
    :mssql 1443))

(defn db-driver 
  "Returns the default database driver"
  [db-type]
  (case db-type 
    :mysql "com.mysql.jdbc.Driver"
    :oracle "oracle.jdbc.driver.OracleDriver"
    :mssql "com.microsoft.jdbc.sqlserver.SQLServerDriver"))

(defn build-connect-string 
  "Builds the connection string for JDBC from parts"
  [db-type host port database-name host user pwd]
  (case db-type
    :mysql (format "//%s:%d/%s?zeroDateTimeBehavior=convertToNull" host port database-name)
    :oracle (format "thin:@%s:%d/%s" host port database-name)
    :mssql (format "//%s:%p;database=%s;user=%s;password=%s" host port database-name user pwd)))

(def db-spec (ref {}))

(defn read-password 
  "Reads password from stdin if not specified in command line parameter"
  []
  (let [console (java.lang.System/console)
        pwd (if console (.readPassword console "password:" nil) 
              (do (print "password:") (flush) (read-line)))]
    (apply str pwd)))

(defn oracle? [] (= *db-type* :oracle))

(defn set-connection-arg 
  "This parses the connection parameters"
  [options]
  (let [{:keys [db-type host port database-name user pwd]} options
        pwd (if-not pwd (read-password) pwd)
        port (if-not port (default-port db-type) port)
        classname (db-driver db-type)]
    (def ^:dynamic *db-type* db-type)
    (dosync (alter db-spec assoc 
                   :subprotocol (name db-type) 
                   :classname classname
                   :user user
                   :password pwd
                   :subname (build-connect-string db-type host port database-name host user pwd)))
    (def pool (pool/make-datasource-spec @db-spec))))

(defn test-orcl-pool 
  "Testing database connection for oracle"
  []
  (def ^:dynamic *db-type* :oracle)
  (def pool (pool/make-datasource-spec {:subprotocol "oracle"
                                        :user "dbtest"
                                        :password "dbtest"
                                        :subname "thin:@localhost:1521/orcl"}))
  (def ^:dynamic *command-options* {:schema-name "DBTEST"}))

(defn test-mysql-pool 
  "Testing database connection for mysql"
  []
  (def pool (pool/make-datasource-spec {:subprotocol "mysql"
                                        :user "test"
                                        :password "test"
                                        :subname "//localhost:3306/xxx?zeroDateTimeBehavior=convertToNull"})))

(defn set-and-test-connection
  "Connects to database with the options"
  [options]
  (set-connection-arg options)
  (def ^:dynamic *command-options* options)
  (let [{:keys [database-name host user]} options]
    (try 
      (log/info (format "Connecting to database %s on host %s with user %s" database-name host user))
      (log/debug (assoc @db-spec :password "***"))
      (with-open [con (sql/get-connection @db-spec)]
        true)
      (catch ^java.sql.SQLException Exception e (log/error (str "Error connecting to database:" (.getMessage e)))))))

(defn stringish?
  "Returns true if field type in value is considered string like in the database"
  [value] 
  (if (#{"VARCHAR" "VARCHAR2" "CHAR" "TEXT"} value) true false))

(defn exact-table-name 
  "Returns a fully specified table name. It works both for field and table definitions."
  [table-def]
  (let [{:keys [table_schem table_name]} table-def]
    (case *db-type*
      :oracle (str table_schem "." table_name)
      :mysql table_name
      :mssql table_name)))

(defn quote-field-name
  "Quotes a value as prefered in the current database"
  [field-name]
  (case *db-type*
    :oracle (str """" field-name  """")
    :mysql (str "`" field-name "`")
    :mssql field-name))

(defn table-count
  "Returns the number of tables visible to current user."
  ([con]
   (let [result (case *db-type*
                  :oracle (sql/query con ["select count(*) as cnt from all_tables"])
                  :mssql (sql/query con ["select count(*) as cnt from sys.tables"]))]
     (:cnt (first result))))
  ([] (sql/with-db-connection [con pool] (table-count con))))

(defn get-tables 
  "Returns a seq of tables visible to current user"
  ([] (sql/with-db-connection [con pool] (doall (get-tables con))))
  ;([con] (doall (resultset-seq (.getTables (.getMetaData (:connection con)) nil nil nil (into-array ["TABLE" "VIEW"]))))))
  ([con] (get-tables con nil))
  ([con table-pattern] (let [results (resultset-seq 
                         (.getTables (.getMetaData (:connection con)) 
                                     nil 
                                     (if-let [opt (:schema-name *command-options*)] opt nil)
                                     ;table-pattern (into-array ["TABLE"])))]
                                     "LCS_%" (into-array ["TABLE"])))]
           (filter #(or (not oracle?) 
                        (not (some #{(:table_schem %)} '("SYS" "SYSTEM")))) 
                   results))))

(defn get-fields 
  "Returns the fields of either a table or every table."
  ([] (get-fields nil))
  ([con-or-table] 
   (cond
     (or (string? con-or-table) (nil? con-or-table)) 
     (sql/with-db-connection [con pool] (doall (get-fields con con-or-table)))
     (connection? con-or-table)
     (get-fields con-or-table nil)
     :else nil))
  ([con table] 
   (resultset-seq (.getColumns (.getMetaData (:connection con)) 
                               nil 
                               (if-let [opt (:schema-name *command-options*)] opt nil)
                               table nil))))

;; {:table_cat "xxx", :table_schem nil, :table_name "jos_vm_product", :column_name "product_id", :data_type 4, :type_name "INT", :column_size 10, :buffer_length 65535, :decimal_digits 0, :num_prec_radix 10, :nullable 0, :remarks "", :column_def nil, :sql_data_type 0, :sql_datetime_sub 0, :char_octet_length nil, :ordinal_position 1, :is_nullable "NO", :scope_catalog nil, :scope_schema nil, :scope_table nil, :source_data_type nil, :is_autoincrement "YES"}

(defn alphanum? [x] (re-matches #"[\$\.\w]+" x))

;; query builders
;; The following codes return specific queries used in the program
(defn- qb*limit-result-set 
  "Result set limiter. By passing a statement string it converts to limits its result to predefined *result-set-limit*"
  ([select-stmt limit]
   (case *db-type*
     :mysql (str select-stmt " limit " limit)
     :oracle (str "select * from (" select-stmt ") where rownum <= " limit)
     :mssql select-stmt)) ;; TODO
  ([select-stmt]
   (qb*limit-result-set select-stmt *result-set-limit*)))

(defn qb*row-count
  "Queries the number of rows in a table"
  [table-name] 
  [(str "select count(*) as result from " table-name)])

(defn qb*non-empty-field-count 
  "Counts the number of rows containing non-empty values in a given field"
  [table-name field-name]
  [(str "select count(*) as result from ~table-name where ? is not null and ? != ''") field-name field-name])

(defn qb*sample-field 
  "Takes a sample of values from a given field"
  ([table-name field-name sample-size]
   {:pre [(alphanum? table-name) (alphanum? field-name)]}
   [(qb*limit-result-set (str "select distinct " field-name " as result from " table-name " where ? is not null") sample-size) field-name])
  ([table-name field-name]
   (qb*sample-field table-name field-name *result-set-limit*)))


(defn qb*verify-table-contains-sensitive-data 
  "Builds a query to verify if fields contain the values given. fields-with-values is a seq of (field-name (possible values...)"
  [table-name fields-with-values]
  (let [quote-one (fn [field-name [match-type fv]]
                    (let [field-name (quote-field-name field-name)]
                    (cond 
                      (number? fv) (str field-name "=" fv)
                      (string? fv) (str "lower(\"" field-name "\")" 
                                        (case match-type
                                          :exact (str "='" fv "'")
                                          :like (str " like '%" fv "%'"))))))
        quote-spec (fn [[field-name & match-specs]]
                     (string/join " or " (map #(quote-one field-name %) match-specs)))
        cond-str (string/join " or " (map quote-spec fields-with-values))]
    [(qb*limit-result-set (str "select * from " table-name " where " (apply str cond-str)))]))

;; field selectors
(defmacro fs*min-length
  [size]
  `(filter #(if (:column_size %1) (> (:column_size %1) ~size) true)))

(defmacro fs*sensitive
  []
  `(filter #(fh/sensitive-field? (:column_name %))))
