# InfluxDB V3

Currently, data is not batched to be sent to the InfluxDB server. Instead,
when a sentence arrives in the queue, it is immediately written to the database.

Instead, batch the writes. They should occur either every X seconds, or when
the queue reaches a certain size, which ever occurs first.