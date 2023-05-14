(ns db-syncer.db
  (:require
   [clojure.java.jdbc :as jdbc]
   [db-syncer.specs :refer [db-types?]]
   [result.core :as result]
   [clojure.string :as str])
  (:import
   (java.lang Runnable)
   (java.util.concurrent ArrayBlockingQueue ThreadPoolExecutor TimeUnit)))

(def ^:dynamic *chunk-size* 100)
(def ^:dynamic *max-workers* 10)

(defn db-spec [s user pass]
  (if-let [[_ dbtype] (re-matches #"jdbc:([^:]+):.+" s)]
    (as-> (keyword dbtype) dbtype
      (if (db-types? dbtype)
        (result/ok {:dbtype dbtype :connection-uri s :user user :password pass})
        (result/error (str "Unsupported dbtype " dbtype))))
    (result/error (str "Invalid DB Url: " s))))

(defprotocol DbClient
  (table-def [this table])
  (table-chunk [this table [min-row max-row] limit])
  (sync-chunk [this chunk dst-table]))

(defn- sql-where-key [table row operator]
  (if (and row (#{:= :< :>} operator))
    (let [operator (str (name operator) "?")]
      (str/join " and " (->> (:pk table)
                             (map :column_name)
                             (map #(str % operator)))))
    "true"))

(defn- key-cols [table]
  (str/join "," (map :column_name (:pk table))))

(defn- key-vals [table row]
  (map-indexed #(first %) (:pk table)))

(def DefaultClient
  {:table-def
   (fn [this table]
     (result/result-of
      (jdbc/with-db-metadata [meta (.-ds this)]
        (let [cols (jdbc/metadata-query (.getColumns meta nil nil table nil))]
          (if (empty? cols)
            (result/error "Invalid table or table with no columns")
            {:name table :cols cols
             :pk (jdbc/metadata-query (.getPrimaryKeys meta nil nil table))})))))

   :table-chunk
   (fn [this table [min-row max-row] limit]
     (let [sql (format "select * from %s where %s and %s order by %s limit ?"
                       (:name table)
                       (sql-where-key table min-row :>)
                       (sql-where-key table max-row :<)
                       (key-cols table))]
       (jdbc/query (.-ds this) (vec (flatten [sql (key-vals table min-row) (key-vals table max-row) limit])) {:as-arrays? true})))})

(deftype GenericClient [ds])

(extend GenericClient DbClient DefaultClient)

(defmulti db-client :dbtype)

(defmethod db-client :default
  [ds]
  (->GenericClient ds))

(deftype PostgresClient [ds])

(extend PostgresClient
  DbClient DefaultClient)

(defn sync-tables! [src-db src-table dst-db dst-table]
  (let [executor (ThreadPoolExecutor. *max-workers* *max-workers* Long/MAX_VALUE TimeUnit/MILLISECONDS (ArrayBlockingQueue. (* 3 *max-workers*)))]
    (loop [chunk (table-chunk src-db src-table nil *chunk-size*)]
      (when-not (empty? chunk)
        (.execute executor (reify Runnable
                             (run [_] (sync-chunk dst-db chunk dst-table))))
        (recur (table-chunk src-db src-table (last chunk) *chunk-size*))))))
