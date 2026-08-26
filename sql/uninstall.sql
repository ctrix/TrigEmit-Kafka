--
-- Removes the TrigEmit-Kafka functions from the server.
--
--     mariadb < sql/uninstall.sql
--
-- Dropping the last of these functions makes MariaDB dlclose the module, which
-- runs its unload hook: the producer is torn down and whatever librdkafka
-- still had queued is flushed, bounded by KAFKA_FLUSH_TIMEOUT_MS. Expect this
-- to take up to that long when the broker is unreachable.
--
-- Uses IF EXISTS throughout so it is safe to run against a partial install.
--

DROP FUNCTION IF EXISTS kafka_connect;
DROP FUNCTION IF EXISTS kafka_connection_param;
DROP FUNCTION IF EXISTS kafka_disconnect;
DROP FUNCTION IF EXISTS kafka_send;
DROP FUNCTION IF EXISTS kafka_stats;
DROP FUNCTION IF EXISTS kafka_info;

--
-- Registrations left over from an older build, which used different names
--
DROP FUNCTION IF EXISTS kafka_set_brokers;
