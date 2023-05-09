(ns db-syncer.db
  (:require
   [clojure.java.jdbc :as jdbc]
   [db-syncer.specs :refer [db-types?]]
   [result.core :as result])
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
  (table-first-chunk [this table])
  (table-next-chunk [this table prev-row])
  (sync-chunk [this chunk dst-table]))

(def DefaultClient
  {:table-def (fn [this table]
                (with-open [con (jdbc/get-connection (.-ds this))]
                  (-> (.getMetaData con)
                      (.getTables nil nil table (into-array ["TABLE" "VIEW"])))))

   :table-first-chunk  (fn [this table]
                         (with-open [con (jdbc/get-connection (.-ds this))]))
   :table-next-chunk (fn [this table prev-row])})

(deftype GenericClient [db-spec])

(extend GenericClient DefaultClient)

(defmulti db-client :dbtype)

(defmethod db-client :default
  [db-spec]
  (jdbc/get-connection db-spec))

(deftype PostgresClient [ds])

(extend PostgresClient
  DbClient DefaultClient)

(defn sync-tables! [src-db src-table dst-db dst-table]
  (let [executor (ThreadPoolExecutor. *max-workers* *max-workers* Long/MAX_VALUE TimeUnit/MILLISECONDS (ArrayBlockingQueue. (* 3 *max-workers*)))]
    (loop [chunk (table-first-chunk src-db src-table)]
      (when-not (empty? chunk)
        (.execute executor (reify Runnable
                             (run [_] (sync-chunk dst-db chunk dst-table))))
        (recur (table-next-chunk src-db src-table (last chunk)))))))
