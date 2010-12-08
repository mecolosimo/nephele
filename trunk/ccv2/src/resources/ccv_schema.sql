-- Complete Composition Vector SQL tables.
--
-- Any changes here need to also be made in EmbeddedVectorSetSQL
--
-- Derby Database:
--      Only supports upto SQL-92, so foreign keys are on tables 
--      not rows; e.g., "CONSTRAINT vs_id_fk REFERENCES vector_set_t"
--
--      For auto-increase use "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"
--
--      TEXT is CLOB(64K)
--
-- MySQL Database:
--      For auto-increase use "AUTO_INCREMENT"
-- 
-- PostgreSQL Database:
--      For auto-increase use ???
--      TEXT is ???
--
-- Author: Marc Colosimo
-- Created: 16 Jan 2007
--
-- No longer store the sequence because they can be very large (>64K)
-- Added seq_length to comp_dist_t
--
-- $Id: ccv_schema.sql 944 2008-06-02 11:49:38Z mcolosimo $

CREATE TABLE vector_set_t (
    vs_id INT PRIMARY KEY AUTO_INCREMENT,
    vs_name VARCHAR(255),
    start_window_size INT NOT NULL,
    stop_window_size INT NOT NULL,
    time_stamp TIMESTAMP

);

CREATE TABLE comp_dist_t (
    cd_id INT PRIMARY KEY AUTO_INCREMENT,
    vs_id INT,
    seq_name VARCHAR(225) NOT NULL,
--    seq_text TEXT NOT NULL,
    seq_length INT NOT NULL,
    time_stamp TIMESTAMP,
    
    FOREIGN KEY (vs_id) REFERENCES vector_set_t (vs_id)
);

CREATE TABLE comp_dist_map_t ( 
    cd_id INT, -- CONSTRAINT cd_id_fk REFERENCES comp_dist_t, 
    window_size INT NOT NULL,
    nmer VARCHAR(25) NOT NULL,
    cnt INT NOT NULL,

    FOREIGN KEY (cd_id) REFERENCES comp_dist_t (cd_id)
);
CREATE INDEX comp_dist_map_nmer_idx on comp_dist_map_t (nmer);
--CREATE INDEX comp_dist_map_window_size_idx on comp_dist_map_t (window_size);

CREATE TABLE comp_vector_t ( 
    cd_id INT NOT NULL, -- CONSTRAINT cd_id_fk REFERENCES comp_dist_t, 
    window_size INT NOT NULL, 
    nmer VARCHAR(25) NOT NULL,
    pi_value DOUBLE NOT NULL,

    FOREIGN KEY (cd_id) REFERENCES comp_dist_t (cd_id)
);
CREATE INDEX comp_vector_nmer_idx on comp_vector_t (nmer);
--CREATE INDEX comp_vector_cd_idx on comp_vector_t (cd_id);