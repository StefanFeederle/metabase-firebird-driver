(ns metabase.driver.firebird
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [honey.sql :as hsql]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
            ;; [metabase.driver.sql-jdbc.sync.describe-database :as sql-jdbc.sync.describe-database]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql-jdbc.sync.common :as sql-jdbc.sync.common]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.util.honey-sql-2 :as hx]
            [metabase.util.ssh :as ssh])
  (:import [java.sql DatabaseMetaData Time]
           [java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]
           [java.sql Connection DatabaseMetaData ResultSet]))

(set! *warn-on-reflection* true)

(defonce ^:private _startup-log
  (do
    (println "[Firebird-driver] ➜ namespace loaded, driver registered")
    true))                                       ; value stored in defonce

(defmethod sql.qp/->honeysql [:firebird :extract]
  [driver [_ unit expr]]
  ;; --- DEBUG ---------------------------------------------------------------
  (println "[Firebird-driver] rewriting EXTRACT for"
           (str/upper-case (name unit)) "←" expr)
  ;; ------------------------------------------------------------------------

  ;; Check if expr is already in Honey SQL format
  (let [expr-honeysql (if (and (vector? expr)
                               (#{:metabase.util.honey-sql-2/typed
                                  :metabase.util.honey-sql-2/identifier
                                  :field :cast :raw} (first expr)))
                        expr  ; Already Honey SQL, use as-is
                        (sql.qp/->honeysql driver expr))  ; Convert to Honey SQL
        [expr-sql & params] (sql.qp/format-honeysql driver expr-honeysql)]
    [:raw (format "EXTRACT(%s FROM %s)"
                  (str/upper-case (name unit))
                  expr-sql)]))

(driver/register! :firebird, :parent :sql-jdbc)

(defn- firebird->spec
  "Create a database specification for a FirebirdSQL database."
  [{:keys [host port db jdbc-flags]
    :or   {host "localhost", port 3050, db "", jdbc-flags ""}
    :as   opts}]
  (merge {:classname   "org.firebirdsql.jdbc.FBDriver"
          :subprotocol "firebirdsql"
          :subname     (str "//" host ":" port "/" db jdbc-flags)}
         (dissoc opts :host :port :db :jdbc-flags)))

;; use Honey SQL 2
(defmethod sql.qp/honey-sql-version :firebird
           [_driver]
           2)

;; Obtain connection properties for connection to a Firebird database.
(defmethod sql-jdbc.conn/connection-details->spec :firebird [_ details]
  (-> details
      (update :port (fn [port]
                      (if (string? port)
                        (Integer/parseInt port)
                        port)))
      (set/rename-keys {:dbname :db})
      firebird->spec
      (sql-jdbc.common/handle-additional-options details)))

(defmethod driver/can-connect? :firebird [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["SELECT 1 FROM RDB$DATABASE"])))))))

;; Use pattern matching because some parameters can have a length parameter, e.g. VARCHAR(255)
(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
    [[#"INT64"            :type/BigInteger]
     [#"DECIMAL"          :type/Decimal]
     [#"FLOAT"            :type/Float]
     [#"BLOB"             :type/*]
     [#"INTEGER"          :type/Integer]
     [#"NUMERIC"          :type/Decimal]
     [#"DOUBLE"           :type/Float]
     [#"SMALLINT"         :type/Integer]
     [#"CHAR"             :type/Text]
     [#"BIGINT"           :type/BigInteger]
     [#"TIMESTAMP"        :type/DateTime]
     [#"DATE"             :type/Date]
     [#"TIME"             :type/Time]
     [#"BLOB SUB_TYPE 0"  :type/*]
     [#"BLOB SUB_TYPE 1"  :type/Text]
     [#"DOUBLE PRECISION" :type/Float]
     [#"BOOLEAN"          :type/Boolean]]))

;; Map Firebird data types to base types
(defmethod sql-jdbc.sync/database-type->base-type :firebird [_ database-type]
  (database-type->base-type database-type))


;(defmethod sql.qp/apply-top-level-clause [:firebird :limit]
;  [_driver _top-level-clause honeysql-form {value :limit}]
;  (-> honeysql-form
;      (dissoc :select)
;      (assoc :select-top (into [(sql.qp/inline-num value)] (:select honeysql-form)))))

;; Firebird  LIMIT  →  SELECT FIRST n …
;(defmethod sql.qp/apply-top-level-clause [:firebird :limit]
;  [ _driver _clause honeysql-form {value :limit}]
;  (-> honeysql-form
;      (dissoc :select)
;      (assoc :select-top (into [(sql.qp/inline-num value)] (:select honeysql-form)))
;      (dissoc :limit)))                        ; suppress trailing LIMIT


;; ────────────────────────────────────────────────────────────
;;  LIMIT n   →  SELECT (FIRST n)
;; ────────────────────────────────────────────────────────────
(defmethod sql.qp/apply-top-level-clause [:firebird :limit]
  [_driver _clause honeysql-form {n :limit}]
  (-> honeysql-form
      (update :select                          ; prepend raw token
              (fnil #(into [{:raw (str "FIRST " n)}] %) []))
      (dissoc :limit)))                        ; suppress trailing LIMIT

;; ────────────────────────────────────────────────────────────
;;  PAGE    →  SELECT FIRST items SKIP offset …
;; ────────────────────────────────────────────────────────────
(defmethod sql.qp/apply-top-level-clause [:firebird :page]
  [_driver _clause honeysql-form {{:keys [items page]} :page}]
  (let [offset (* items (dec page))]
    (-> honeysql-form
        (update :select
                (fnil #(into [{:raw (str "FIRST " items " SKIP " offset)}] %) []))
        (dissoc :limit :offset))))              ; suppress LIMIT/OFFSET

;; Fix for relative datetime operations that generate parameters
(defmethod sql.qp/->honeysql [:firebird :relative-datetime]
  [driver [_ amount unit]]
  (let [current-time (sql.qp/current-datetime-honeysql-form driver)]
    (sql.qp/add-interval-honeysql-form driver current-time amount unit)))

;; Override the honeysql formatting to catch all extract operations at the SQL generation level
(defmethod sql.qp/format-honeysql :firebird [_driver honeysql-form]
  (let [formatted (sql.qp/format-honeysql :sql-jdbc honeysql-form)]
    (if (vector? formatted)
      (let [[sql & params] formatted
            ;; Replace FIRST(n) with FIRST n 
            firebird-sql (str/replace sql #"SELECT\s+\(FIRST\s+(\d+)\)," "SELECT FIRST $1 ")
            ;; Fix EXTRACT syntax more carefully to preserve parameter structure
            ;; Handle quoted units: EXTRACT("UNIT", expr) -> EXTRACT(UNIT FROM expr)
            firebird-sql (str/replace firebird-sql 
                                     #"EXTRACT\s*\(\s*\"([^\"]+)\"\s*,\s*([^)]+)\)" 
                                     "EXTRACT($1 FROM $2)")
            ;; Handle unquoted units: EXTRACT(UNIT, expr) -> EXTRACT(UNIT FROM expr)  
            ;; Be more specific to avoid affecting parameters
            firebird-sql (str/replace firebird-sql 
                                     #"EXTRACT\s*\(\s*([A-Z_]+)\s*,\s*(?![\?\d])" 
                                     "EXTRACT($1 FROM ")]
        ;; Return with all original parameters preserved
        (cons firebird-sql params))
      formatted)))

;; When selecting constants Firebird doesn't check privileges, we have to select all fields
(defn simple-select-probe-query
  [driver schema table]
  {:pre [(string? table)]}
  (let [honeysql {:select [:*]
                  :from   [(sql.qp/->honeysql driver (hx/identifier :table schema table))]
                  :where  [:not= 1 1]}
        honeysql (sql.qp/apply-top-level-clause driver :limit honeysql {:limit 0})]
    (sql.qp/format-honeysql driver honeysql)))

(defn- execute-select-probe-query
  [driver ^Connection conn [sql & params]]
  {:pre [(string? sql)]}
  (with-open [stmt (sql-jdbc.sync.common/prepare-statement driver conn sql params)]
    ;; attempting to execute the SQL statement will throw an Exception if we don't have permissions; otherwise it will
    ;; truthy wheter or not it returns a ResultSet, but we can ignore that since we have enough info to proceed at
    ;; this point.
    (.execute stmt)))

(defmethod sql-jdbc.sync/have-select-privilege? :firebird
  [driver conn table-schema table-name]
  ;; Query completes = we have SELECT privileges
  ;; Query throws some sort of no permissions exception = no SELECT privileges
  (let [sql-args (simple-select-probe-query driver table-schema table-name)]
    (try
      (execute-select-probe-query driver conn sql-args)
      true
      (catch Throwable _
        false))))

(defmethod sql-jdbc.sync/active-tables :firebird [& args]
  (apply sql-jdbc.sync/post-filtered-active-tables args))

;; Convert unix time to a timestamp
(defmethod sql.qp/unix-timestamp->honeysql [:firebird :seconds] [_ _ expr]
  [:DATEADD [:raw "SECOND"] expr (hx/cast :TIMESTAMP (hx/literal "01-01-1970 00:00:00"))])

;; Helpers for Date extraction
;; TODO: This can probably simplified a lot by using String concentation instead of
;; replacing parts of the format recursively

;; Specifies what Substring to replace for a given time unit
(defn- get-unit-placeholder [unit]
  (case unit
    :SECOND :ss
    :MINUTE :mm
    :HOUR   :hh
    :DAY    :DD
    :MONTH  :MM
    :YEAR   :YYYY))

(defn- get-unit-name [unit]
  (case unit
    0 :SECOND
    1 :MINUTE
    2 :HOUR
    3 :DAY
    4 :MONTH
    5 :YEAR))

;; ---------------------------------------------------------------------------
;; helper that returns  EXTRACT(YEAR FROM "source"."CIN_INVOICEDATE")
;; ---------------------------------------------------------------------------
(defn- extract-sql [driver unit expr]
  ;; FIXED: Check if expr is already compiled to avoid double compilation
  (let [expr-honeysql (if (and (vector? expr)
                               (#{:metabase.util.honey-sql-2/typed
                                  :metabase.util.honey-sql-2/identifier
                                  :field :cast :raw :dateadd :replace} (first expr)))
                        expr  ; Already in Honey SQL format
                        (sql.qp/->honeysql driver expr))  ; Convert to Honey SQL
        [expr-sql & _] (sql.qp/format-honeysql driver expr-honeysql)]
    (format "EXTRACT(%s FROM %s)"
            (str/upper-case (name unit))
            expr-sql)))

;; --------------------------------------------------------------------------
;; REPLACE the part of the timestamp string with EXTRACT(...)
;; --------------------------------------------------------------------------
(defn- replace-timestamp-part [input unit expr]
  [:replace
   input
   (hx/literal (get-unit-placeholder unit))      ; the placeholder stays literal
   [:raw (extract-sql :firebird unit expr)]])    ; final SQL → no further processing

(defn- format-step [expr input step wanted-unit]
  (if (> step wanted-unit)
    (format-step expr (replace-timestamp-part input (get-unit-name step) expr) (- step 1) wanted-unit)
    (replace-timestamp-part input (get-unit-name step) expr)))

(defn- format-timestamp [expr format-template wanted-unit]
  (format-step expr (hx/literal format-template) 5 wanted-unit))

;; Firebird doesn't have a date_trunc function, so use a workaround: First format the timestamp to a
;; string of the wanted resulution, then convert it back to a timestamp
(defn- timestamp-trunc [expr format-str wanted-unit]
  (hx/cast :TIMESTAMP (format-timestamp expr format-str wanted-unit)))

(defn- date-trunc [expr format-str wanted-unit]
  (hx/cast :DATE (format-timestamp expr format-str wanted-unit)))

;; Helper function to safely convert expressions to honeysql, avoiding double compilation
(defn- safe->honeysql [driver expr]
  (if (and (vector? expr)
           (keyword? (first expr))
           (#{:metabase.util.honey-sql-2/typed
              :metabase.util.honey-sql-2/identifier
              :field :cast :raw :dateadd :replace :extract} (first expr)))
    expr  ; Already in Honey SQL format
    (sql.qp/->honeysql driver expr)))  ; Convert to Honey SQL

(defmethod sql.qp/date [:firebird :default]         [_ _ expr] expr)
;; Cast to TIMESTAMP if we need minutes or hours, since expr might be a DATE
(defmethod sql.qp/date [:firebird :minute]          [_ _ expr] (timestamp-trunc (hx/cast :TIMESTAMP expr) "YYYY-MM-DD hh:mm:00" 1))
(defmethod sql.qp/date [:firebird :minute-of-hour]  [driver _ expr] (safe->honeysql driver [:extract :MINUTE (hx/cast :TIMESTAMP expr)]))
(defmethod sql.qp/date [:firebird :hour]            [_ _ expr] (timestamp-trunc (hx/cast :TIMESTAMP expr) "YYYY-MM-DD hh:00:00" 2))
(defmethod sql.qp/date [:firebird :hour-of-day]     [driver _ expr] (safe->honeysql driver [:extract :HOUR (hx/cast :TIMESTAMP expr)]))
;; Cast to DATE to get rid of anything smaller than day
(defmethod sql.qp/date [:firebird :day]             [_ _ expr] (hx/cast :DATE expr))
;; Firebird DOW is 0 (Sun) - 6 (Sat); increment this to be consistent with Java, H2, MySQL, and Mongo (1-7)
(defmethod sql.qp/date [:firebird :day-of-week]     [driver _ expr] (hx/+ (safe->honeysql driver [:extract :WEEKDAY (hx/cast :DATE expr)]) 1))
(defmethod sql.qp/date [:firebird :day-of-month]    [driver _ expr] (safe->honeysql driver [:extract :DAY expr]))
;; Firebird YEARDAY starts from 0; increment this
(defmethod sql.qp/date [:firebird :day-of-year]     [driver _ expr] (hx/+ (safe->honeysql driver [:extract :YEARDAY expr]) 1))
;; Cast to DATE because we do not want units smaller than days
;; Use :raw for DAY in dateadd because the keyword :WEEK gets surrounded with quotations
(defmethod sql.qp/date [:firebird :week]            [driver _ expr] [:dateadd [:raw "DAY"] (hx/- 0 (safe->honeysql driver [:extract :WEEKDAY (hx/cast :DATE expr)])) (hx/cast :DATE expr)])
(defmethod sql.qp/date [:firebird :week-of-year]    [driver _ expr] (safe->honeysql driver [:extract :WEEK expr]))
(defmethod sql.qp/date [:firebird :month]           [_ _ expr] (date-trunc expr "YYYY-MM-01" 4))
(defmethod sql.qp/date [:firebird :month-of-year]   [driver _ expr] (safe->honeysql driver [:extract :MONTH expr]))
;; Use :raw for MONTH in dateadd because the keyword :MONTH gets surrounded with quotations
(defmethod sql.qp/date [:firebird :quarter]         [driver _ expr] [:dateadd [:raw "MONTH"] (hx/* (hx// (hx/- (safe->honeysql driver [:extract :MONTH expr]) 1) 3) 3) (date-trunc expr "YYYY-01-01" 5)])
(defmethod sql.qp/date [:firebird :quarter-of-year] [driver _ expr] (hx/+ (hx// (hx/- (safe->honeysql driver [:extract :MONTH expr]) 1) 3) 1))
(defmethod sql.qp/date [:firebird :year]            [driver _ expr] (safe->honeysql driver [:extract :YEAR expr]))

;; Firebird 2.x doesn't support TRUE/FALSE, replacing them with 1 and 0
(defmethod sql.qp/->honeysql [:firebird Boolean]    [_ bool] (if bool 1 0))

;; TODO / Help Wanted:
;; Firebird 2.x doesn't support SUBSTRING arugments seperated by commas, but uses FROM and FOR keywords
;(defmethod sql.qp/->honeysql [:firebird :substring]
;  [driver [_ arg start length]]
;  (let [col-name (hformat/to-sql (sql.qp/->honeysql driver arg))]
;    (if length
;      (reify
;        hformat/ToSql
;        (to-sql [_]
;          (str "substring(" col-name " FROM " start " FOR " length ")")))
;      (reify
;        hformat/ToSql
;        (to-sql [_]
;          (str "substring(" col-name " FROM " start ")"))))))

(defmethod sql.qp/add-interval-honeysql-form :firebird [driver hsql-form amount unit]
  (if (= unit :quarter)
    (recur driver hsql-form (hx/* amount 3) :month)
    [:dateadd [:raw (name unit)] amount hsql-form]))

(defmethod sql.qp/current-datetime-honeysql-form :firebird [_]
  [:metabase.util.honey-sql-2/typed
   [:cast [:metabase.util.honey-sql-2/literal "now"] [:raw "timestamp"]]
   {:database-type "timestamp"}])

(defmethod sql.qp/->honeysql [:firebird :stddev]
  [driver [_ field]]
  [:stddev_samp (sql.qp/->honeysql driver field)])

;; MEGA HACK based on sqlite driver

(defn- zero-time? [t]
  (= (t/local-time t) (t/local-time 0)))

(defmethod sql.qp/->honeysql [:firebird LocalDate]
  [_ t]
  (hx/cast :DATE (t/format "yyyy-MM-dd" t)))

(defmethod sql.qp/->honeysql [:firebird LocalDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    (hx/cast :TIMESTAMP (t/format "yyyy-MM-dd HH:mm:ss.SSSS" t))))

(defmethod sql.qp/->honeysql [:firebird LocalTime]
  [_ t]
  (hx/cast :TIME (t/format "HH:mm:ss.SSSS" t)))

(defmethod sql.qp/->honeysql [:firebird OffsetDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    (hx/cast :TIMESTAMP (t/format "yyyy-MM-dd HH:mm:ss.SSSS" t))))

(defmethod sql.qp/->honeysql [:firebird OffsetTime]
  [_ t]
  (hx/cast :TIME (t/format "HH:mm:ss.SSSS" t)))

(defmethod sql.qp/->honeysql [:firebird ZonedDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    (hx/cast :TIMESTAMP (t/format "yyyy-MM-dd HH:mm:ss.SSSS" t))))

(doseq [[feature supported?] {; supported
                              :basic-aggregations                      true
                              :expression-aggregations                 true
                              :foreign-keys                            true
                              :nested-queries                          true
                              :standard-deviation-aggregations         true
                              ; not supported
                              :binning                                 false
                              :case-sensitivity-string-filter-options  false
                              :nested-fields                           false
                              :schemas                                 false
                              :set-timezone                            false}]
  (defmethod driver/database-supports? [:firebird feature] [_driver _feature _db] supported?))