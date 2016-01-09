(ns anon-valid.main
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.tools.logging :as log] 
            [clojure.term.colors :refer :all]
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [anon-valid.field-handler :as fh]
            [anon-valid.cache :as c]
            [anon-valid.core :as core]
            [anon-valid.db :as db]))

(def version "0.6.0")

(def commands (atom []))

(def ^:dynamic *cache-file* nil)

(def ^:dynamic *options* nil)

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
   ["-x" "--ex COMMAND" "Execute a command" :id :execute-command :default "test-connection"
    :validate-fn #((command-names) %)]
   ["-d" "--db-name DATABASE_NAME" "Database name" :id :database-name]
   ["-s" "--schema-name SCHEMA_NAME" "Schema name" :id :schema-name]
   ["-D" "--data-file DATA_FILE_OR_DIRECTORY" "Loads sensitive data definitions from file or directory" :id :data-file ]
   ["-c" "--cache CACHE" "Loads cached result from file. Program will overwrite the cache with the result. A backup is created in .bkp file." :id :cache-file ]
   ["-H" "--host HOST" "Database host" :id :host :default "localhost"]
   ["-t" "--db-type TYPE" "Type of database, default is mysql" :id :db-type :default :mysql
    :parse-fn #(keyword %)
    :validate-fn #(#{:mysql :oracle :mssql} %)]
   ["-P" "--password PASSWORD" "Database password, if not given asked from the command line" :id :pwd]
   ["-p" "--port PORT" "Database port, default is database type dependent" :id :port]
   ["-u" "--user USERNAME" "Database user" :id :user]
   ["-g" "--generate SCRIPTNAME" "Generate anonimization script" :id :script-name]
   ["-f" "--formatting FORMATTING" "Use FORMATTING where possible" :id :formating :default :pretty]
   ["-v" "--version" "Print program version" :id :version ]])

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

(defn- std-progress
  "Used to display progress information on the stdout"
  [ & args] 
  (if *cache-file* (c/cache args))
  (println args))

(command sf scan-fields "Searching for fields with sensitive content"
  (println "searching for fields containing sensitive data")
  (core/scan-for-fields-with-sensitive-values std-progress))

(command tc test-connection "Test database connection"
  (println "success!"))

(command st sensitive-tables "Searching for tables with sensitive content"
  (println "Searching for tables containing sensitive data")
  (core/tables-with-sensitive-values std-progress))

(command trc row-counts "Count the number of rows in tables"
  (println "Counting rows in tables")
  (core/table-row-counts))

(command sat sample-tables "Sample suspected fields of tables"
  (println "Sample table suspected sensitive fields")
  (core/sample-sensitive-fields))

(command dfd dump-field-definitions "Dump field definitions"
  (println "Dump table field definitions of schema " (*options* :schema-name))
  (println (count (core/dump-field-definitions std-progress))))

(defn execute-command [options]
  (let [cmd-name (symbol (:execute-command options))]
    ((first (filter #(let [m-cmd (meta %)]
                    (or (= cmd-name (:name m-cmd))
                        (= cmd-name (:short-command-name m-cmd)))) @commands)))))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (def ^:dynamic *options* options)
    (if (:version options)
      (exit 0 (str "program version " version)))
    (cond
      (or (:help options) (nil? (:execute-command options))) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors))
      (nil? (:user options)) (exit 1 "Missing user name for connection")
      (nil? (:database-name options)) (exit 1 "Missing database name")
      (not (db/set-and-test-connection options)) (exit 1 ""))
    (if-let [data-file (:data-file options)]
      (if (some false? (fh/load-sdata data-file))
        (exit 1 (str "Error parsing file " data-file)))
      (do (log/info "Loading standard definitions...")
        (fh/load-definitions)))
    (if-let [cache-file (:cache-file options)]
      (try
        (def ^:dynamic *cache-file* cache-file)
        (log/info "Loading cache file" cache-file)
        (c/load-cache cache-file)
        (catch Exception e
          (log/warn "Error reading cache file" cache-file))))

    (execute-command options)
    (if *cache-file* (c/write-cache *cache-file*))))
