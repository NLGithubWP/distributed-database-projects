1. Give user select and update privilege

    GRANT all on database cs5424 to naili;
    GRANT all on TABLE cs5424.public.* to naili;
    SHOW GRANTS ON DATABASE cs5424;
    SHOW GRANTS ON TABLE cs5424.public.*;


2. create indexs on some tables

    CREATE INDEX s_joint_id ON stock (s_w_id, s_i_id);
    CREATE INDEX c_joint_id ON customer (c_w_id, c_d_id, c_id);
    CREATE INDEX d_joint_id ON district (d_w_id, d_id);
    CREATE INDEX order_ori_joint_id ON order_ori (o_w_id, o_d_id, o_id, o_carrier_id);
    CREATE INDEX order_ori_c_id ON order_ori (o_c_id);
    CREATE INDEX order_ori_entry_d ON order_ori (o_entry_d);
    CREATE INDEX order_line_joint_id ON order_line (ol_w_id, ol_d_id, ol_o_id, ol_number);


