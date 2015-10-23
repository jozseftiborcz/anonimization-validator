(ns anon-valid.main
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.tools.logging :as log] 
            [clojure.term.colors :refer :all]
            [clojure.pprint :as pp]
            [anon-valid.field-handler :as fh]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [anon-valid.core :as core]
            [anon-valid.db :as db]))

(def version "0.0.2")

(def commands (atom []))

(defmacro command [cmd-short-name cmd-long-name cmd-doc & body] 
  `(do
     (defn ~cmd-long-name [] ~@body) 
     (alter-meta! (resolve '~cmd-long-name) assoc :short-command-name '~cmd-short-name :doc ~cmd-doc) 
     (swap! commands conj (resolve '~cmd-long-name))))

(defn command-descriptions[]
  (apply str (map #(format "  %-30s %s\n" 
                           (str (:name (meta %)) " (" (:short-command-name (meta %)) ")") 
                           (:doc (meta %))) @commands)))

(defn command-names[]
  (reduce #(conj %1 (str (:name (meta %2))) (str (:short-command-name (meta %2)))) #{} @commands))

(def cli-options
  [["-h" "--help" "Print this help" :id :help]
   ["-x" "--ex COMMAND" "Execute a command" :id :execute-command :default "sample-sensitive-fields"
    :validate-fn #((command-names) %)]
   ["-d" "--db-name DATABASE_NAME" "Database or schema name" :id :database-name]
   ["-D" "--data-file DATA_FILE_OR_DIRECTORY" "Loads sensitive data definitions from file or directory" :id :data-file ]
   ["-c" "--cache CACHE" "Loads cached result from file. Program will overwrite the cache with the result. A backup is created in .bkp file." :id :cache-file ]
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
        (command-descriptions)
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

(command sf scan-fields "Searching for fields with sensitive content"
  (let [progress-fn (fn [ & args] (println args))]
    (println "searching for fields containing sensitive data")
    (core/scan-for-fields-with-sensitive-values progress-fn)))

(command st sensitive-tables "Searching for tables with sensitive content"
  (let [progress-fn (fn[stage & args]
                      (let [table-spec (first args)]
                        (case stage
                          :start (println "searching for tables containing sensitive data")
                          :not-sensitive (println "table " (:table_name table-spec) " is anonim")
                          :sensitive (println (red "table " (:table_name table-spec) " is not anonim")))))]
    (pp/pprint (core/tables-with-sensitive-values progress-fn))))

(command trc row-counts "Count the number of rows in tables"
  (let [progress-fn (fn[stage & args]
                      (let [table-spec (first args)]
                        (case stage
                          :start (println "searching for tables containing sensitive data")
                          :not-sensitive (println "table " (:table_name table-spec) " is anonim")
                          :sensitive (println (red "table " (:table_name table-spec) " is not anonim")))))]
    (core/table-row-counts)))

(command sat sample-tables "Sample suspected fields of tables"
  (let [progress-fn (fn[stage & args]
                      (let [table-spec (first args)]
                        (case stage
                          :start (println "searching for tables containing sensitive data")
                          :not-sensitive (println "table " (:table_name table-spec) " is anonim")
                          :sensitive (println (red "table " (:table_name table-spec) " is not anonim")))))]
    (core/sample-sensitive-fields)))

(defn execute-command [options]
  (let [cmd-name (symbol (:execute-command options))]
    ((first (filter #(let [m-cmd (meta %)]
                    (or (= cmd-name (:name m-cmd))
                        (= cmd-name (:short-command-name m-cmd)))) @commands)))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (log/info "Hello, World!")
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (or (:help options) (nil? (:execute-command options))) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors))
      (nil? (:user options)) (exit 1 "Missing user name for connection")
      (nil? (:database-name options)) (exit 1 "Missing schema name")
      (not (db/set-and-test-connection options)) (exit 1 ""))
    (if (:data-file options) 
      (do (when-not (fh/load-sdata (:data-file options))
            (exit 1 "Error parsing file " (:data-file options))))
      (do (log/info "Loading standard definitions...")
        (fh/load-definitions)))
    (execute-command options)))
