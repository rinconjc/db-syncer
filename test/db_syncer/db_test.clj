(ns db-syncer.db-test
  (:require [db-syncer.db :as sut]
            [clojure.test :as t]
            [result.core :as r]))

;; (t/use-fixtures :once (fn [f]
;;                         ()))

(t/deftest can-parse-db-url []
  (doseq [[url expected] [["jdbc:postgresql://localhost:5432/db1?user=postgres&password=pass123" r/ok?]
                          ["jdbc:unknown://localhost:11122" r/error?]
                          ["jdbc:mysql://localhost:3306/mydb?user=root&password=pass123" r/ok?]]]
    (t/is (expected (sut/db-spec url nil nil)))))

(t/deftest can-create-db-client []
  (let [db-spec (sut/db-spec "jdbc:postgresql://localhost:5432/db1?user=postgres&password=pass123" nil nil)]
    (t/is (some? (sut/db-client (r/ok? db-spec))))))
