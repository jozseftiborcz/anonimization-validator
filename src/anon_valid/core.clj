(ns anon-valid.core
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] 
         '[clojure.tools.logging :as log] 
         '[jdbc.pool.c3p0 :as pool] 
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as field-handler])

(def db-spec {:subprotocol "mysql"
              :subname "//localhost:3306/xxx"
              :user "test"
              :password "test"})

(def pool
  (pool/make-datasource-spec db-spec))

(defn get-tables []
  (sql/with-db-connection [con pool] (doall (resultset-seq (.getTables (.getMetaData (:connection con)) nil nil nil (into-array ["TABLE" "VIEW"]))))))

(defn get-fields [table] 
  (sql/with-db-connection [con pool] (doall (resultset-seq (.getColumns (.getMetaData (:connection con)) nil nil table nil)))))

(defn get-field-names [table]
  (map :column_name (get-fields table)))


