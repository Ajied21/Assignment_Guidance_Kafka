# Dibimbing-Ajied, Data Engineering Bootcamp

# Kafka

<div style="text-align: center;">
  <img src="https://kafka.apache.org/images/apache-kafka.png" width="350">
</div>

1. Clone This Repo.
2. Run `make kafka` and Run `make Jupyter`

---
```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## jupyter                      - Spinup jupyter notebook for testing and validation purposes.
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## clean                        - Cleanup all running containers related to the challenge.
```

# Dokumentasi

- Topic ada 4:
  - Data_Transaksi_Keuangan_E-commerce
  - AVG_AGE_PER_CATEGORY
  - MERCHANT_REVENUE
  - PAYMENT_METHOD_COUNTS
<div style="text-align: center;">
    <img src="./images/Topic.png" alt="Architecture Overview" width="500"/>
</div>

- Masing-masing topic ada 5 partition
<div style="text-align: center;">
    <img src="./images/Partition.png" alt="Architecture Overview" width="500"/>
</div>

- Consumer
<div style="text-align: center;">
    <img src="./images/Consumer.png" alt="Architecture Overview" width="500"/>
</div>

- KSQL DB
<div style="text-align: center;">
    <img src="./images/KSQLDB.png" alt="Architecture Overview" width="500"/>
</div>
