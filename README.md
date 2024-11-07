# Dibimbing-Ajied, Data Engineering Bootcamp

# Kafka

<div style="text-align: center;">
  <img src="https://kafka.apache.org/images/apache-kafka.png" width="350">
</div>

# Deskripsi Data
Project ini bertujuan untuk menghasilkan data transaksi e-commerce secara acak dan mengirimkannya ke topik Kafka yang telah ditentukan. Script ini dapat digunakan untuk melakukan simulasi aliran data transaksi dalam sistem Kafka, cocok untuk pengujian dan pengembangan aplikasi yang memerlukan data transaksi real-time.

- Fitur
  - Menghasilkan data transaksi e-commerce dengan atribut seperti ID transaksi, waktu, jumlah transaksi, metode pembayaran, dan lainnya.
  - Mengirim data transaksi ke Kafka dengan pembagian partisi berdasarkan transaction_id.
  - Menggunakan jeda waktu (5 detik) antar pengiriman data untuk mensimulasikan aliran data secara bertahap.

# Run
1. Clone This Repo.
2. Run `make kafka` and Run `make Jupyter`

---
```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## jupyter                      - Spinup jupyter notebook for testing and validation purposes.
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## clean                        - Cleanup all running containers related to the challenge.
```

# Struktur Data
- transaction_id: ID unik untuk setiap transaksi yang dihasilkan dengan UUID.
- timestamp: Tanggal dan waktu transaksi dibuat dalam format ISO.
- user_id: ID unik pengguna yang melakukan transaksi, juga dihasilkan dengan UUID.
- merchant_id: ID unik dari pedagang, dihasilkan dengan UUID.
- amount: Jumlah transaksi dalam format desimal, dengan dua angka desimal.
- age: Usia pengguna (antara 19 hingga 65 tahun).
- gender: Jenis kelamin pengguna, baik 'Male' atau 'Female'.
- phone_number: Nomor telepon pengguna.
- email: Alamat email pengguna.
- currency: Kode mata uang yang digunakan dalam transaksi.
- location: Nama kota pengguna.
- country: Negara tempat pengguna berada.
- payment_method: Metode pembayaran yang digunakan dalam transaksi (contoh: "Credit Card", "PayPal").
- product_id: ID unik untuk produk dalam transaksi, dihasilkan dengan UUID.
- product_category: Kategori produk yang dibeli (contoh: "Electronics and Gadgets", "Fashion and Accessories").
- quantity: Jumlah item produk yang dibeli.
- discount: Diskon yang diterapkan pada transaksi dalam format desimal.
- tax: Pajak yang diterapkan pada transaksi dalam format desimal.
- shipping_cost: Biaya pengiriman yang ditambahkan pada transaksi dalam format desimal.
- total_cost: Total biaya setelah menghitung jumlah, diskon, pajak, dan biaya pengiriman.

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
