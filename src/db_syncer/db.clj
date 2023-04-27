(ns db-syncer.db
  (:require
   [clojure.spec.alpha :as spec]
   [db-syncer.specs :as s]
   [next.jdbc :as jdbc]))

(defn db-spec [s]
  (when-let [[_ dbtype] (re-matches #"jdbc:([^:]+):.+" s)]
    {:dbtype (keyword dbtype) :db-url s}))

(defmulti db-conn :dbtype)

(defmethod db-conn :default
  [db-spec]
  (jdbc/get-connection db-spec))

(defmulti table-def :dbtype)

(defmethod table-def :postgres
  [db-spec]
  (with-open [conn (jdbc/get-connection db-spec)]
    (jdbc/execute! "select from information_schema.tables where table_name=?" )))

(defmulti table-first-chunk :dbtype)

(defn sync-tables! [src-db src-table dst-db dst-table]
  (let [chunk (table-first-chunk src-db src-table chunk-size)]
    (thread (process chunk dst-db dst-table) )
    )
  (with-open [src-conn (jdbc/get-connection src-db)]
    (with-open [dst-conn (jdbc/get-connection dst-db)]
      (let []))))
