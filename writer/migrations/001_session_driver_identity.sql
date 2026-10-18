CREATE TABLE IF NOT EXISTS session_driver_identity (
    driver_identity_id BIGINT NOT NULL AUTO_INCREMENT,
    session_id INT NOT NULL,
    game_car_id INT NOT NULL,
    user_id INT NOT NULL,
    driver_name VARCHAR(100) NOT NULL DEFAULT '',
    connected_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (driver_identity_id),
    INDEX idx_session_driver_identity_lookup (session_id, game_car_id, connected_at),
    INDEX idx_session_driver_identity_user (session_id, user_id),
    CONSTRAINT fk_session_driver_identity_session FOREIGN KEY (session_id)
        REFERENCES session (session_id) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS session_stint_driver_identity (
    session_stint_id INT NOT NULL,
    driver_identity_id BIGINT NOT NULL,
    PRIMARY KEY (session_stint_id),
    INDEX idx_stint_driver_identity_identity (driver_identity_id),
    CONSTRAINT fk_stint_driver_identity_stint FOREIGN KEY (session_stint_id)
        REFERENCES session_stint (session_stint_id) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT fk_stint_driver_identity_identity FOREIGN KEY (driver_identity_id)
        REFERENCES session_driver_identity (driver_identity_id) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB;
