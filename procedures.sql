1. Give user select and update privilege

    GRANT all on database cs5424 to naili;
    GRANT all on TABLE cs5424.public.* to naili;
    SHOW GRANTS ON DATABASE cs5424;
    SHOW GRANTS ON TABLE cs5424.public.*;


2. create indexs on some tables

    CREATE INDEX s_joint_id ON stock (s_w_id, s_i_id);
    CREATE INDEX c_joint_id ON customer (c_w_id, c_d_id, c_id);
    CREATE INDEX d_joint_id ON district (d_w_id, d_id);


