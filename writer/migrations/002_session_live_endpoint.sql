CREATE TABLE IF NOT EXISTS session_live_endpoint (
    session_id INT NOT NULL,
    node_id VARCHAR(128) NOT NULL,
    public_http_url VARCHAR(2048) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (session_id),
    CONSTRAINT fk_session_live_endpoint_session FOREIGN KEY (session_id)
        REFERENCES session (session_id) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB;
