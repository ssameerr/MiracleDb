//! PostgreSQL CDC connector (logical replication stub).
//!
//! Connects to a PostgreSQL instance using a logical replication slot and
//! streams `ChangeEvent`s into a Tokio channel.  The full wire protocol
//! implementation requires `tokio-postgres` with the `replication` feature;
//! this stub wires the channel plumbing and returns an empty stream so that
//! the rest of the system compiles and can be tested end-to-end.

/// Reads logical-replication WAL from a PostgreSQL replication slot and
/// converts decoded messages into `ChangeEvent`s.
pub struct PostgresCDC {
    /// `postgresql://user:pass@host/db` connection string.
    pub connection_string: String,
    /// Name of the logical replication slot to consume.
    pub slot_name: String,
}

impl PostgresCDC {
    /// Create a new `PostgresCDC` connector.
    ///
    /// # Arguments
    /// * `connection_string` - A libpq-style connection URI.
    /// * `slot_name` - The name of an existing logical replication slot
    ///   (created with `pg_create_logical_replication_slot`).
    pub fn new(connection_string: &str, slot_name: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
            slot_name: slot_name.to_string(),
        }
    }

    /// Start streaming changes from PostgreSQL.
    ///
    /// Returns a `Receiver` that callers can `await` to receive `ChangeEvent`s
    /// as they are committed to the WAL.
    ///
    /// # Current behaviour
    /// The full implementation connects to PostgreSQL logical replication using
    /// the streaming replication protocol (START_REPLICATION … LOGICAL) and
    /// decodes pgoutput / wal2json messages into `ChangeEvent`s.  That path
    /// depends on `tokio-postgres` with the `replication` feature flag which
    /// is not yet wired in.  For now the method creates the channel and
    /// immediately returns the receiver without spawning a producer; this
    /// keeps the rest of the codebase compilable and allows integration tests
    /// to be written against the channel API.
    pub async fn start(
        &self,
    ) -> Result<tokio::sync::mpsc::Receiver<super::cdc::ChangeEvent>, String> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1000);

        // TODO: spawn a task that connects to `self.connection_string`,
        // creates / attaches to replication slot `self.slot_name`, and
        // forwards decoded ChangeEvents via `_tx`.  Example skeleton:
        //
        // ```ignore
        // let conn_str = self.connection_string.clone();
        // let slot = self.slot_name.clone();
        // tokio::spawn(async move {
        //     let (client, conn) =
        //         tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        //             .await
        //             .expect("connect");
        //     tokio::spawn(conn);
        //     // … START_REPLICATION / decode loop …
        // });
        // ```

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_cdc_new() {
        let cdc = PostgresCDC::new("postgresql://localhost/mydb", "miracledb_slot");
        assert_eq!(cdc.connection_string, "postgresql://localhost/mydb");
        assert_eq!(cdc.slot_name, "miracledb_slot");
    }

    #[tokio::test]
    async fn start_returns_receiver() {
        let cdc = PostgresCDC::new("postgresql://localhost/test", "test_slot");
        let rx = cdc.start().await.expect("start");
        // Channel is empty but valid — no events produced by the stub.
        assert!(rx.is_empty());
    }
}
