CREATE TABLE aus
(
    id    integer not null,
    label integer,
    data1 float,
    data2 float,
    data3 float,
    data4 float,
    data5 float,
    data6 float,
    data7 float,
    data8 float,
    data9 float,
    data10 float,
    data11 float,
    data12 float,
    data13 float,
    data14 float,
    PRIMARY KEY(id) 
);

CREATE TABLE weights 
(
    weight1 float,
    weight2 float,
    weight3 float,
    weight4 float,
    weight5 float,
    weight6 float,
    weight7 float,
    weight8 float,
    weight9 float,
    weight10 float,
    weight11 float,
    weight12 float,
    weight13 float,
    weight14 float
);

PARTITION TABLE aus ON COLUMN id;

CREATE PROCEDURE FROM CLASS lr.procedures.Solve;
PARTITION PROCEDURE Solve ON TABLE aus COLUMN id;
