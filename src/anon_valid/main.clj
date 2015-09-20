(ns anon-valid.main
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(require '[clojure.tools.logging :as log] 
         '[clojure.term.colors :refer :all]
         '[anon-valid.field-handler :as fh]
         '[clojure.string :as string]
         '[clojure.tools.cli :as cli]
         '[anon-valid.core :as core]
         '[anon-valid.db :as db])

(def version "0.0.2")

(def cli-options
  [["-h" "--help" "Print this help" :id :help]
   ["-x" "--ex COMMAND" "Execute a command" :id :execute-command :default "sample-sensitive-fields"
    :validate-fn #(#{"table-row-counts" "sensitive-fields" "sample-sensitive-fields" "tables-with-sensitive-values" "twsv"} %)]
   ["-d" "--db-name DATABASE_NAME" "Database or schema name" :id :database-name]
   ["-H" "--host HOST" "Database host" :id :host :default "localhost"]
   ["-t" "--db-type TYPE" "Type of database, default is mysql" :id :db-type :default :mysql
    :parse-fn #(keyword %)
    :validate-fn #(#{:mysql :oracle :mssql} %)]
   ["-P" "--password PASSWORD" "Database password, if not given asked from the command line" :id :pwd]
   ["-p" "--port PORT" "Database port, default is database type dependent" :id :port]
   ["-u" "--user USERNAME" "Database user" :id :user]
   ["-f" "--formatting FORMATTING" "Use FORMATTING where possible" :id :formating :default :pretty]])

(defn usage [options-summary]
  (->> ["Anonimization validator. It validates a database via JDBC connection that sensitive data is deleted."
        ""
        "Usage: program-name [options]"
        ""
        "Options:"
        options-summary
        ""
        "POSSIBLE COMMANDS"
        ""
        "table-row-counts     List non empty tables"
        ""
        "FORMATTING"
        "pretty               pretty printing for human consumption"
        "csv                  concise for post processing"
        "ignore               to build ignore list"
        ""]
    (string/join \newline)))


(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (if msg (println msg))
  (System/exit status))

(defn execute-command [options]
  (case (:execute-command options) 
    "table-row-counts" (core/table-row-counts)
    "sensitive-fields" (core/print-sensitive-fields)
    "sample-sensitive-fields" (core/sample-sensitive-fields)
    (list "tables-with-sensitive-values" "twsv") (core/tables-with-sensitive-values)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (log/info "Hello, World!")
  (fh/load-definitions)
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (or (:help options) (nil? (:execute-command options))) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors))
      (nil? (:user options)) (exit 1 "Missing user name for connection")
      (nil? (:database-name options)) (exit 1 "Missing schema name")
      (not (db/set-and-test-connection options)) (exit 1 ""))
    (execute-command options)))
;  (log/info (db/tables-with-sensitive-fields))
  ;(println (sql/query connect-db ["select * from jos_vm_module"]))
  ;(log/info (core/get-tables))
  ;(log/info (map :column_name (core/get-fields "jos_vm_product")))
