FROM apache/spark:3.5.1-python3

USER root

# psycopg2 for the upsert-based meter_last_state writes and alert inserts.
RUN pip install --no-cache-dir psycopg2-binary==2.9.9

WORKDIR /opt/app
COPY speed_layer.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "--master", "local[2]", \
     "/opt/app/speed_layer.py"]
