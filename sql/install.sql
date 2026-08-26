--
-- Registers the TrigEmit-Kafka functions with the server.
--
--     mariadb < sql/install.sql
--
-- kafka.so must already be in the server's plugin directory (SELECT
-- @@plugin_dir) before this runs; "cmake --install build" puts it there.
--
-- Registration is server-wide and persists across restarts: it is a row in
-- mysql.func, not a session setting. It needs INSERT on mysql.func, which in
-- practice means running this as root. There is no need to USE mysql first --
-- CREATE FUNCTION ... SONAME registers globally from any database.
--

CREATE FUNCTION kafka_connect           RETURNS INTEGER SONAME 'kafka.so';
CREATE FUNCTION kafka_connection_param  RETURNS INTEGER SONAME 'kafka.so';
CREATE FUNCTION kafka_disconnect        RETURNS INTEGER SONAME 'kafka.so';
CREATE FUNCTION kafka_send              RETURNS INTEGER SONAME 'kafka.so';
CREATE FUNCTION kafka_stats             RETURNS STRING  SONAME 'kafka.so';
CREATE FUNCTION kafka_info              RETURNS STRING  SONAME 'kafka.so';
