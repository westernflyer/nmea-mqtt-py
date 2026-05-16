Missing sentences (none of them are needed):
  - DTM
  - GNS
  - HDG
  - HDM
  - MWD
  - ROT
  - VBW
  - VWH
  - ZDA

The present schema is wide and very sparse, with lots of nulls. A better schema would be:

```
RSA,mmsi=368323170,talker=II rudder_angle=-1.6 1778932895310
HDT,mmsi=368323170,talker=HE hdg_true=93.3 1778932895323
MWV,mmsi=368323170,talker=FT awa=218.0,aws_knots=10.8 1778932895333
MDA,mmsi=368323170,talker=II pressure_inches=30.3,pressure_bars=1.025,temperature_air_celsius=8.5,twd_true=292.19,tws_knots=9.39,tws_mps=4.83,pressure_millibars=1025.0 1778932895369
DPT,mmsi=368323170,talker=SD depth_below_transducer_meters=1.24,transducer_depth_meters=0.0,water_depth_meters=1.24 1778932895859
MWV,mmsi=368323170,talker=FT awa=217.0,aws_knots=10.8 1778932895859
MWV,mmsi=368323170,talker=WI awa=205.49,aws_knots=9.29 1778932895893
```

This puts each sentence type in its own table.