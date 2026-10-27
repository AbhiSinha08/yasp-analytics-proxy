;; Run the bundled Metabase driver's connection and Redshift definition queries.
(require '[cheshire.core :as json])
(try
  (require 'metabase.driver.postgres 'metabase.driver.redshift)
  (let [profile (System/getenv "YASP_JDBC_PROFILE")
      schema (or (System/getenv "YASP_JDBC_SCHEMA") "PUBLIC")
      analytics (if (= schema "PUBLIC") "ANALYTICS" "analytics")
      orders-schema (if (= schema "PUBLIC") "PUBLIC" "analytics")
      orders (if (= schema "PUBLIC") "ORDERS" "orders")
      order-id (if (= schema "PUBLIC") "ID" "id")
      amount (if (= schema "PUBLIC") "AMOUNT" "amount")
      relation (str "\"" orders-schema "\".\"" orders "\"")
      driver (if (= profile "redshift") :redshift :postgres)
      details {:host (System/getenv "YASP_JDBC_HOST")
               :port (Integer/parseInt (System/getenv "YASP_JDBC_PORT"))
               :dbname (System/getenv "YASP_JDBC_DATABASE")
               :user (System/getenv "YASP_JDBC_USER")
               :password (System/getenv "YASP_JDBC_PASSWORD")
               :ssl true
               :additional-options "preferQueryMode=simple&compression=off&ApplicationName=yasp&ssl=true&sslmode=verify-ca&sslrootcert=/tmp/yasp-fixture-ca.crt"}
      connected ((requiring-resolve 'metabase.driver/can-connect?) driver details)
      spec ((requiring-resolve 'metabase.driver.sql-jdbc.connection/connection-details->spec) driver details)
      query (requiring-resolve 'clojure.java.jdbc/query)
      generated (mapv #(update % :amount str)
                      (query spec [(format "-- Metabase:: userID: 1 queryType: MBQL queryHash: abc\nSELECT %s.\"%s\" AS \"%s\", %s.\"%s\" AS \"%s\" FROM %s ORDER BY %s.\"%s\" LIMIT 2000"
                                           relation order-id order-id relation amount amount relation relation order-id)]))
      metadata (when (= driver :redshift)
                 (let [fields (requiring-resolve 'metabase.driver.sql-jdbc.sync/describe-fields-sql)
                       fks (requiring-resolve 'metabase.driver.sql-jdbc.sync/describe-fks-sql)]
                   {:public_fields (query spec (fields driver :schema-names [schema]))
                    :analytics_fields (query spec (fields driver :schema-names [analytics]))
                    :order_fields (query spec (fields driver :schema-names [orders-schema] :table-names [orders]))
                    :foreign_keys (query spec (fks driver))
                    :filtered_foreign_keys (query spec (fks driver :schema-names [schema analytics]))}))
      native (with-open [connection ((requiring-resolve 'clojure.java.jdbc/get-connection) spec)
                         statement (.createStatement connection)
                         rows (.executeQuery statement "-- Metabase:: userID: 1 queryType: native queryHash: abc\nSELECT 1")]
               (.next rows)
               {:jdbc_type (.getColumnType (.getMetaData rows) 1)
                :value (str (.getObject rows 1))})]
    (println (str "RESULT " (json/generate-string
                             {:connected connected :native native :metadata metadata :generated generated}))))
  (catch Throwable error
    (println (str "RESULT " (json/generate-string {:connected false :error (.getMessage error)}))))
  (finally
    (shutdown-agents)
    (System/exit 0)))
