(ns metabase.driver.firebird
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [honey.sql :as hsql]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
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

(driver/register! :firebird, :parent :sql-jdbc)

(defonce ^:private _startup-log
  (do
    (println "[Firebird-driver] ➜ namespace loaded, driver registered")
    true))

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

;; Use "FIRST" instead of "LIMIT"
(defmethod sql.qp/apply-top-level-clause [:firebird :limit] [_ _ honeysql-form {value :limit}]
  (assoc honeysql-form :modifiers [(format "FIRST %d" value)]))

;; Use "SKIP" instead of "OFFSET"
(defmethod sql.qp/apply-top-level-clause [:firebird :page] [_ _ honeysql-form {{:keys [items page]} :page}]
  (assoc honeysql-form :modifiers [(format "FIRST %d SKIP %d"
                                           items
                                           (* items (dec page)))]))

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
    (.execute stmt)))

(defmethod sql-jdbc.sync/have-select-privilege? :firebird
  [driver conn table-schema table-name]
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

;; FIXED: Remove the problematic global extract method and handle it properly in date functions
;; The original method was interfering with other expressions

;; Helpers for Date extraction - UPDATED for better handling
(defn- extract-firebird [unit expr]
  "Create a Firebird EXTRACT expression"
  [:raw (format "EXTRACT(%s FROM %s)"
                (str/upper-case (name unit))
                ;; Use a placeholder that will be replaced during formatting
                "%%EXPR%%")])

(defn- firebird-extract-sql [driver unit expr]
  "Generate proper Firebird EXTRACT SQL"
  (let [expr-sql (first (hsql/format-expr (sql.qp/->honeysql driver expr)
                                          {:quoted true}))]
    (format "EXTRACT(%s FROM %s)"
            (str/upper-case (name unit))
            expr-sql)))

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

;; REPLACE the part of the timestamp string with EXTRACT(...)
(defn- replace-timestamp-part [driver input unit expr]
  [:replace
   input
   (hx/literal (get-unit-placeholder unit))
   [:raw (firebird-extract-sql driver unit expr)]])

(defn- format-step [driver expr input step wanted-unit]
  (if (> step wanted-unit)
    (format-step driver expr (replace-timestamp-part driver input (get-unit-name step) expr) (- step 1) wanted-unit)
    (replace-timestamp-part driver input (get-unit-name step) expr)))

(defn- format-timestamp [driver expr format-template wanted-unit]
  (format-step driver expr (hx/literal format-template) 5 wanted-unit))

;; Firebird doesn't have a date_trunc function, so use a workaround
(defn- timestamp-trunc [driver expr format-str wanted-unit]
  (hx/cast :TIMESTAMP (format-timestamp driver expr format-str wanted-unit)))

(defn- date-trunc [driver expr format-str wanted-unit]
  (hx/cast :DATE (format-timestamp driver expr format-str wanted-unit)))

;; UPDATED date methods to pass driver parameter
(defmethod sql.qp/date [:firebird :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:firebird :minute]          [driver _ expr] (timestamp-trunc driver (hx/cast :TIMESTAMP expr) "YYYY-MM-DD hh:mm:00" 1))
(defmethod sql.qp/date [:firebird :minute-of-hour]  [_ _ expr] [:extract :MINUTE (hx/cast :TIMESTAMP expr)])
(defmethod sql.qp/date [:firebird :hour]            [driver _ expr] (timestamp-trunc driver (hx/cast :TIMESTAMP expr) "YYYY-MM-DD hh:00:00" 2))
(defmethod sql.qp/date [:firebird :hour-of-day]     [_ _ expr] [:extract :HOUR (hx/cast :TIMESTAMP expr)])
(defmethod sql.qp/date [:firebird :day]             [_ _ expr] (hx/cast :DATE expr))
(defmethod sql.qp/date [:firebird :day-of-week]     [_ _ expr] (hx/+ [:extract :WEEKDAY (hx/cast :DATE expr)] 1))
(defmethod sql.qp/date [:firebird :day-of-month]    [_ _ expr] [:extract :DAY expr])
(defmethod sql.qp/date [:firebird :day-of-year]     [_ _ expr] (hx/+ [:extract :YEARDAY expr] 1))
(defmethod sql.qp/date [:firebird :week]            [_ _ expr] [:dateadd [:raw "DAY"] (hx/- 0 [:extract :WEEKDAY (hx/cast :DATE expr)]) (hx/cast :DATE expr)])
(defmethod sql.qp/date [:firebird :week-of-year]    [_ _ expr] [:extract :WEEK expr])
(defmethod sql.qp/date [:firebird :month]           [driver _ expr] (date-trunc driver expr "YYYY-MM-01" 4))
(defmethod sql.qp/date [:firebird :month-of-year]   [_ _ expr] [:extract :MONTH expr])
(defmethod sql.qp/date [:firebird :quarter]         [driver _ expr] [:dateadd [:raw "MONTH"] (hx/* (hx// (hx/- [:extract :MONTH expr] 1) 3) 3) (date-trunc driver expr "YYYY-01-01" 5)])
(defmethod sql.qp/date [:firebird :quarter-of-year] [_ _ expr] (hx/+ (hx// (hx/- [:extract :MONTH expr] 1) 3) 1))
(defmethod sql.qp/date [:firebird :year]            [_ _ expr] [:extract :YEAR expr])

;; Firebird 2.x doesn't support TRUE/FALSE, replacing them with 1 and 0
(defmethod sql.qp/->honeysql [:firebird Boolean] [_ bool] (if bool 1 0))

;; UPDATED: Better interval handling
(defmethod sql.qp/add-interval-honeysql-form :firebird [driver hsql-form amount unit]
  (if (= unit :quarter)
    (recur driver hsql-form (hx/* amount 3) :month)
    [:dateadd [:raw (name unit)] amount hsql-form]))

(defmethod sql.qp/current-datetime-honeysql-form :firebird [_]
  (hx/cast :timestamp (hx/literal :now)))

(defmethod sql.qp/->honeysql [:firebird :stddev]
  [driver [_ field]]
  [:stddev_samp (sql.qp/->honeysql driver field)])

;; Time handling methods
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

;; UPDATED: Database capabilities for latest Metabase
(doseq [[feature supported?] {;; Supported features
                              :basic-aggregations                      true
                              :expression-aggregations                 true
                              :foreign-keys                            true
                              :nested-queries                          true
                              :standard-deviation-aggregations         true
                              :left-join                               true
                              :right-join                              true
                              :inner-join                              true
                              :full-join                               false ;; Firebird doesn't support FULL OUTER JOIN
                              :case-sensitivity-string-filter-options  true
                              :regex                                   false
                              
                              ;; Unsupported features
                              :binning                                 false
                              :nested-fields                           false
                              :schemas                                 false
                              :set-timezone                            false
                              :window-functions                        false
                              :percentile-aggregations                 false}]
  (defmethod driver/database-supports? [:firebird feature] [_driver _feature _db] supported?))