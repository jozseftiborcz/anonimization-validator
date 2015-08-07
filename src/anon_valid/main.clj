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

(def version "0.0.1")

(def cli-options
  [["-h" "--help" "Print this help" :id :help]
   ["-x" "--ex COMMAND" "Execute a command" :id :execute-command
    :validate-fn #(#{"table-row-counts" "sensitive-fields"} %)
    ]
   ["-p" "--pretty" "Use pretty formatting where possible" :id :pretty-formating]])

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
        ""]
    (string/join \newline)))


(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (log/info "Hello, World!")
  (fh/load-definitions)
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors)))
    (case (:execute-command options) 
      "table-row-counts" (core/table-row-counts)
      "sensitive-fields" (core/sensitive-fields)))
;  (log/info (db/tables-with-sensitive-fields))
  ;(println (sql/query connect-db ["select * from jos_vm_module"]))
  ;(log/info (core/get-tables))
  ;(log/info (map :column_name (core/get-fields "jos_vm_product")))
  )
