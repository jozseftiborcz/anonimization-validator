(ns anon-valid.main
  (:gen-class)
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:require [clojure.tools.logging :as log] 
            [clojure.term.colors :refer :all]
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [anon-valid.field-handler :as fh]
            [anon-valid.sdata-server :as sds]
            [anon-valid.core :as core]
            [anon-valid.db :as db]))

(def version "0.6.1")

(def commands (atom []))

(def ^:dynamic *cache-file* nil)
(def the-cache (atom {}))
(def ^:dynamic *result-file* nil)
(def ^:dynamic *options* nil)

(defmacro command [cmd-short-name cmd-long-name cmd-doc & body] 
  `(do
     (defn ~cmd-long-name [] 
       (log/info '~cmd-doc)
       ~@body) 
     (alter-meta! (resolve '~cmd-long-name) assoc :short-command-name '~cmd-short-name :doc ~cmd-doc) 
     (swap! commands conj (resolve '~cmd-long-name))))

(defn command-descriptions[]
  (apply str (map #(format "  %-40s %s\n" 
                           (str (:name (meta %)) " (" (:short-command-name (meta %)) ")") 
                           (:doc (meta %))) @commands)))

(defn command-names[]
  (reduce #(conj %1 (str (:name (meta %2))) (str (:short-command-name (meta %2)))) #{} @commands))

(def cli-options
  [["-h" "--help" "Print this help" :id :help]
   ["-x" "--ex COMMAND" "Execute a command" :id :execute-command :default "test-connection"
    :validate-fn #((command-names) %)]
   ["-m" "--mode MODENAME" "Call program with one of the pre-configured mode" :id :mode :default :none]
   ["-d" "--db-name DATABASE_NAME" "Database name" :id :database-name]
   ["-s" "--schema-name SCHEMA_NAME" "Schema name" :id :schema-name]
   ["-D" "--data-file DATA_FILE_OR_DIRECTORY" "Loads sensitive data definitions from file or directory" :id :data-file ]
   ["-H" "--host HOST" "Database host" :id :host :default "localhost"]
   ["-t" "--db-type TYPE" "Type of database, default is mysql" :id :db-type :default :mysql
    :parse-fn #(keyword %)
    :validate-fn #(#{:mysql :oracle :mssql} %)]
   ["-P" "--password PASSWORD" "Database password, if not given asked from the command line" :id :pwd]
   ["-p" "--port PORT" "Database port, default is database type dependent" :id :port]
   ["-u" "--user USERNAME" "Database user" :id :user]
   ["-r" "--result RESULTFILE" "write log file in format (default csv)" :id :result-file]
   ["-R" nil "same as -l but log file name will be the default one (dbname_schemaname.log)" :id :result-file
    :assoc-fn (fn[m k _] (assoc m :result-file :default-result-file))]
   ["-f" "--formatting FORMATTING" "result formatting (csv, json clj)" :id :formating :default :csv]
   ["-c" "--cache CACHEFILE" "use cached result from file. Program will overwrite the cache with the new result. A backup is created in .bkp file." :id :cache-file
    :validate-fn #(#{:csv :json :clj} %)]
   ["-C" nil "same as -c but cache file will be the default one" :id :cache-file 
    :assoc-fn (fn[m k _] (assoc m :cache-file :default-cache-file))]
   ["-v" nil "Verbosity levels, can be specified multiple times: (-vvvv) no info and no log (-vvv) no info and log, (-vv) info and log, -v debug and log" :id :verbosity :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ["-o" "--server-port PORT" "port used to listen on" :id :server-port :default 8080]
   ["-g" "--generate SCRIPTNAME" "Generate anonimization script" :id :script-name]
   ["-V" "--version" "Print program version" :id :version ]])

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
  (if msg (log/info msg))
  (System/exit status))

(defn cache
  ([k v]
   (if *cache-file* 
     (swap! the-cache assoc k v)))
  ([k] 
   (if *cache-file*
     (get @the-cache k))))

(defn write-result
  [& args]
  (if *result-file*
    (do 
      (.write *result-file* (apply str args))
      (.write *result-file* "\n")
      (.flush *result-file*))))

(defn- std-progress
  "Used to display progress information on the stdout"
  [ stage & args] 
  (if-not (or (.startsWith (str stage) ":cached") (nil? args)) (log/info args))
  (if-not (#{:start :end} stage) (write-result (string/join ";" args))))

(defn- field-progress[ & out-fields]
  (let [first-row? (atom true)
        fld-count (dec (count out-fields))]
    (fn [stage & args] 
      (if @first-row?
        (do (reset! first-row? false)
          (write-result (str "schema;table-name;column-name;" (string/join ";" out-fields)))))
      (let [rest-str (string/join "," (flatten (nthrest (rest args) fld-count)))
            log-str (fn[x] (let [{:keys [table_schem table_name column_name]} (first x)]
                             (flatten (list table_schem table_name column_name 
                                            (take fld-count (rest args)) 
                                            rest-str))))]
        (if-not (or (.startsWith (str stage) ":cached") (nil? args)) (log/info (log-str args)))
        (if-not (#{:start :end} stage) (write-result (string/join ";" (log-str args))))))))

(command sf scan-fields "Searching for fields with sensitive content"
  (core/scan-fields-with-sensitive-values cache (field-progress "sensitive-data" "sample")))

(command tc test-connection "Test database connection"
  (log/info "success!"))

(defn st-progress[]
  (let [first-row? (atom true)]
    (fn[stage & args]
      (case stage
        (:table-sensitivity :cached-table-sensitivity)
        (do
          (if @first-row?
            (do (reset! first-row? false)
              (write-result "table_name;table_schem;sensitive?")))
          (if (= stage :table-sensitivity) (log/info args))
          (write-result (string/join ";" args)))
        nil))))
(command st sensitive-tables "Searching for tables with sensitive content"
  (core/mark-tables-sensitivity cache (st-progress)))

(command trc row-counts "Count the number of rows in tables"
  (core/table-row-counts cache std-progress))

(command ssfn sample-sensitive-field-names "Sample sensitive field names"
  (core/sample-sensitive-field-names))

(command lsfc list-sensitive-field-candidates "List fields which might contain sensitive values"
  (core/list-sensitive-field-candidates cache (field-progress "sensitive-data")))

(defn dfd-progress[]
  (let [first-row? (atom false)]
    (fn[stage & args]
      (case stage
        :start 
        (reset! first-row? true)
        :table-start
        (log/info (:table_name (first args)))
        (:table-field-definition :cached-table-field-definition)
        (let [field-writer (fn[field] 
                             (doall (map #(field %) (sort (keys field)))))
              fields (first args)]
          (if @first-row?
            (do (reset! first-row? false)
              (log/info (sort(keys(first fields))))
              (write-result (string/join ";" (sort(keys(first fields)))))))
          (doall (map #(do 
                         (write-result (string/join ";" (field-writer %)))) fields)))
        nil))))

(command dfd dump-field-definitions 
         "Dump field definitions of schema"
         (core/dump-field-definitions cache (dfd-progress)))

(defn dtd-progress 
  [stage & args]
  (let [{:keys [table_schem table_type table_name]} (first args)]
    (case stage
      :start 
          (do 
            (log/info "table-schema;table-type;table-name")
            (write-result "table-schema;table-type;table-name"))
      :table-definition 
        (do (log/info (str table_schem "." table_type "." table_name))
          (write-result table_schem ";" table_type ";" table_name))
      :cached-table-definition
        (write-result table_schem ";" table_type ";" table_name)
      nil)))

(command dtd dump-table-definitions 
         "Dump tables of schema" 
         (core/dump-table-definitions cache dtd-progress))

(command sds sensitive-data-server "Sensitive-data-server"
  (sds/start-sdata-server (*options* :server-port)))

(defn execute-command [options]
  (let [cmd-name (symbol (:execute-command options))]
    ((first (filter #(let [m-cmd (meta %)]
                    (or (= cmd-name (:name m-cmd))
                        (= cmd-name (:short-command-name m-cmd)))) @commands)))))

(defmacro sdata-handler
  [cmd]
  `(do (if-let [data-file# (:data-file *options*)]
         (if (some false? (fh/load-sdata data-file#))
           (exit 1 (str "Error parsing file " data-file#)))
         (do (log/info "Loading standard definitions...")
           (fh/load-definitions)))
     ~cmd))

(defn default-file-name
  [extension]
  (str (*options* :database-name) "_" 
       (*options* :schema-name) "_"
       (*options* :execute-command)
       extension))

(defmacro result-handler 
  [cmd]
   `(if-let [result-file# (if (= (:result-file *options*)
                                :default-result-file)
                            (default-file-name ".res")
                            (:result-file *options*))]
      (do 
        (log/info "Writing result to file" result-file#)
        (with-open [file# (io/writer result-file#)]
          (binding [*result-file* file#]
           ~cmd)))
     ~cmd))

(defn write-cache
  "Writes the cache to file and creates a backup"
  [file]
  (let [bkp (io/file (str file ".bkp"))
        file (io/file file)]
    (if (.exists file)
      (io/copy file bkp))
    (spit file @the-cache)))

(defmacro cache-handler
  [cmd]
  `(if-let [cache-file# (if (= (:cache-file *options*)
                               :default-cache-file)
                          (default-file-name ".cache")
                          (:cache-file *options*))]
     (do 
       (def ^:dynamic *cache-file* cache-file#)
       (if (.exists (io/as-file cache-file#))
         (try
           (log/info "Loading cache file" cache-file#)
           (reset! the-cache (read-string (slurp cache-file#)))
           (catch Exception e#
             (log/warn "Error reading cache file" cache-file# e#))))
       ~cmd
       (if *cache-file* (write-cache *cache-file*)))
     ~cmd))

(defmacro connection-handler
  [cmd]
  `(if (db/set-and-test-connection *options*)
    ~cmd
    (exit 1 "")))

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
      (nil? (:database-name options)) (exit 1 "Missing database name"))
    (println *options*)
    (->>
      (execute-command options)
      result-handler
      cache-handler
      sdata-handler
      connection-handler)))


