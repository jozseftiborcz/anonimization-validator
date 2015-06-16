(ns anon-valid.core
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.java.jdbc :as sql] '[clojure.tools.logging :as log])

(def db-spec {:subprotocol "mysql"
              :subname "//localhost:3306/xxx"
              :user "test"
              :password "test"})

(defn pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec)) 
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))] 
    {:datasource cpds}))

;;(def pooled-db (delay (pool db-spec)))

;;(defn db-connection [] @pooled-db)

(defn get-tables-x []
  (sql/with-db-connection [db-connect db-spec] (doall (resultset-seq (.getTables (.getMetaData (:connection db-connect)) nil nil nil (into-array ["TABLE" "VIEW"]))))))
(def get-tables (memoize get-tables-x))

(defn get-fields-x [table] 
  (sql/with-db-connection [con db-spec] (doall (resultset-seq (.getColumns (.getMetaData (:connection con)) nil nil table nil)))))

(def get-fields (memoize get-fields-x))

(defn log [level & message]
  (println message))

(defn print-field-counts []
  )

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  ;;(println (sql/query connect-db ["select * from jos_vm_module"]))
  (println (count (get-tables)))
  (println (get-fields "jos_vm_product"))
  )
