-- init.sql
CREATE TABLE IF NOT EXISTS tec_data (
    id SERIAL PRIMARY KEY,
    loc VARCHAR(255),
    loc_zn VARCHAR(255),
    loc_name VARCHAR(255),
    loc_purp_desc VARCHAR(255),
    loc_qti VARCHAR(255),
    flow_ind VARCHAR(10),
    dc INTEGER,
    opc INTEGER,
    tsq INTEGER,
    oac INTEGER,
    it BOOLEAN,
    auth_overrun_ind BOOLEAN,
    nom_cap_exceed_ind BOOLEAN,
    all_qty_avail BOOLEAN,
    qty_reason VARCHAR(255),
    cycle INTEGER
);
