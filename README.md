# Distributed DNS Lookup (Master–Worker)

Proyek UAS Komputasi Paralel dan Sistem Terdistribusi, tema 15: Distributed DNS Lookup.
Master memecah daftar domain menjadi task, lalu Worker di tiap node meresolusinya secara paralel
(multiprocessing) dan mengirim hasilnya kembali lewat jaringan.

```
                +--------------------+
                |      master.py     |  pecah task, load balancing,
                |  (1 laptop)        |  deteksi gagal, hitung metrik
                +---------+----------+
                          | TCP (JSON per baris), port 5000
        +-----------------+-----------------+
        |                 |                 |
  +-----+-----+     +-----+-----+     +-----+-----+
  | worker.py |     | worker.py | ... | worker.py |   (maks. 9 node)
  | Pool core |     | Pool core |     | Pool core |
  +-----------+     +-----------+     +-----------+
```

## 1. Kompatibilitas OS

| OS | Status |
|---|---|
| Linux | Sudah diuji (Python 3.12, Master + 3 Worker lewat localhost, termasuk simulasi Worker crash) |
| Windows | Kode memakai library standar yang lintas-OS dan `multiprocessing` (mode *spawn*) sudah diperhitungkan, **tetapi belum diuji langsung**. Lakukan uji singkat di tiap laptop Windows sebelum hari-H (lihat bagian 4) |

Master dan Worker boleh berbeda OS (mis. Master di Linux, sebagian Worker di Windows), karena
komunikasinya hanya teks JSON lewat TCP.

Catatan khusus Windows:
- Gunakan `python` atau `py` (bukan `python3`).
- Pasang **dnspython**. Tanpanya, status `NXDOMAIN` di Windows bisa terbaca sebagai `ERROR`
  karena kode error socket Windows berbeda.
- `Ctrl+C` kadang telat merespons saat Worker menunggu Master. Pakai `Ctrl+Break`
  atau tutup jendela terminal.
- Saat pertama dijalankan, Windows Defender Firewall akan memunculkan dialog. Pilih **Allow** (Master wajib).

## 2. Persiapan (semua node, Master maupun Worker)

Kebutuhan: Python 3.8+ dan salin **kedua file** (`master.py` dan `worker.py`) ke folder yang sama
di Master. Worker cukup memiliki `worker.py`.

```bash
# Linux
python3 -m pip install dnspython psutil

# Windows (PowerShell / CMD)
py -m pip install dnspython psutil
```

`dnspython` dan `psutil` sebenarnya opsional, tetapi `dnspython` memberi timeout per query dan
bisa menentukan DNS server, sedangkan `psutil` dipakai untuk metrik CPU/Memory.

### Dataset domain

Gunakan sumber yang bisa dirujuk di laporan, misalnya **Tranco List** (https://tranco-list.eu).
Format `rank,domain` langsung didukung.

```bash
curl -L -o top-1m.csv.zip https://tranco-list.eu/top-1m.csv.zip
unzip top-1m.csv.zip
head -n 20000 top-1m.csv > domains.txt      # ambil 20.000 domain teratas
```

Di laporan, tuliskan tanggal unduh dan ID/versi daftar Tranco yang dipakai.

## 3. Menjalankan

### 3.1 Cek jaringan

1. Semua perangkat harus berada di jaringan yang sama.
2. Cari IP laptop Master: `ip -4 a` (Linux) atau `ipconfig` (Windows). Contoh: `192.168.1.10`.
3. Buka port 5000 di firewall Master:
   ```bash
   # Linux (ufw)
   sudo ufw allow 5000/tcp
   # Linux (Fedora/firewalld)
   sudo firewall-cmd --add-port=5000/tcp
   # Windows (PowerShell sebagai Administrator)
   netsh advfirewall firewall add rule name="DNS Master" dir=in action=allow protocol=TCP localport=5000
   ```
4. Dari Worker, tes koneksi setelah Master jalan:
   ```bash
   nc -zv 192.168.1.10 5000                       # Linux
   Test-NetConnection 192.168.1.10 -Port 5000     # Windows PowerShell
   ```

Kalau antarperangkat tidak bisa saling ping, kemungkinan Wi-Fi memakai *client isolation*.
Coba hotspot HP atau router/switch sendiri.

### 3.2 Menjalankan Master (laptop Master)

```bash
python3 master.py --mode distributed --domains domains.txt --limit 20000 --min-workers 3 --lb dynamic
```
Master akan menunggu sampai `--min-workers` Worker terhubung, baru mulai membagi task.

### 3.3 Menjalankan Worker (tiap node Worker)

```bash
python3 worker.py --master 192.168.1.10 --port 5000
```
Ganti IP dengan IP Master. Secara bawaan Worker memakai semua core. Batasi dengan `--processes 2` bila perlu.

### 3.4 Urutan lengkap satu sesi

1. Jalankan Master (3.2).
2. Jalankan Worker satu per satu di tiap node (3.3). Master menampilkan `Worker terdaftar: ...`.
3. Setelah jumlah Worker mencapai `--min-workers`, proses otomatis mulai.
4. Setelah selesai, Master menampilkan ringkasan dan menyimpan hasil ke folder `results/`. Worker berhenti sendiri.

## 4. Uji cepat di satu laptop (sebelum ke lab)

Buka 4 terminal di folder yang sama:

```bash
# Terminal 1
python3 master.py --domains domains.txt --limit 3000 --min-workers 2
# Terminal 2 dan 3
python3 worker.py --master 127.0.0.1 --processes 2
python3 worker.py --master 127.0.0.1 --processes 2
```
Kalau ringkasan muncul dan `results/results_*.csv` terbentuk, instalasi di laptop tersebut sudah benar.
Ulangi di laptop Windows dan Linux yang akan dipakai.

## 5. Skenario pengujian sesuai ketentuan UAS

Jalankan **sequential paling awal**, karena Speedup dihitung dari `results/metrics_sequential.json`.

| Pengujian | Perintah |
|---|---|
| Sequential (baseline) | `python3 master.py --mode sequential --domains domains.txt --limit 20000` |
| Local parallel | `python3 master.py --mode local --local-processes 4 --domains domains.txt --limit 20000` |
| Distributed | `python3 master.py --min-workers 9 --domains domains.txt --limit 20000` |
| Load balancing (sesudah) | tambahkan `--lb dynamic` |
| Load balancing (sebelum) | tambahkan `--lb static` |
| Worker lambat (opsional) | di satu Worker: `python3 worker.py --master IP --simulate-delay 0.01` |
| Fault tolerance | di satu Worker: `python3 worker.py --master IP --crash-after 3`, atau matikan paksa prosesnya |
| Scalability | ulangi dengan `--min-workers 1`, `2`, `4`, `6`, `9` |

Untuk uji scalability, nyalakan **hanya sebanyak N Worker** pada tiap percobaan. Worker ekstra yang sudah
terhubung sebelum Master mulai akan ikut dihitung. Agar hasil lebih kuat, ulangi tiap skenario 3 kali
lalu ambil rata-ratanya.

## 6. Parameter penting

**master.py**

| Parameter | Bawaan | Keterangan |
|---|---|---|
| `--mode` | `distributed` | `sequential`, `local`, atau `distributed` |
| `--domains` | (wajib) | File daftar domain |
| `--limit` | 0 (semua) | Batasi jumlah domain |
| `--chunk-size` | 500 | Domain per task |
| `--port` | 5000 | Port Master |
| `--min-workers` | 1 | Mulai setelah N Worker terhubung |
| `--lb` | `dynamic` | `dynamic` atau `static` |
| `--hb-timeout` | 6 | Detik tanpa kabar sebelum Worker dianggap mati |
| `--task-timeout` | 120 | Batas waktu satu task |
| `--max-retries` | 3 | Percobaan ulang maksimum per task |
| `--dns-timeout` | 3.0 | Timeout per query DNS |
| `--nameserver` | bawaan OS | Mis. `8.8.8.8` atau IP resolver lokal |
| `--threads` | 1 | Thread per proses Worker |
| `--tag` | otomatis | Nama berkas keluaran |
| `--baseline-time` | otomatis | Waktu sequential manual (detik) |

**worker.py**

| Parameter | Bawaan | Keterangan |
|---|---|---|
| `--master` | (wajib) | IP Master |
| `--port` | 5000 | Port Master |
| `--processes` | semua core | Jumlah proses |
| `--threads` | ikut Master | Thread per proses |
| `--name` | hostname | Nama Worker di laporan |
| `--crash-after` | 0 | Simulasi crash setelah N task |
| `--simulate-delay` | 0 | Simulasi Worker lambat (detik per domain) |

## 7. Keluaran

Tersimpan di folder `results/`:
- `results_<tag>.csv`: hasil per domain (domain, status, IP, latency, Worker yang memproses)
- `metrics_<tag>.json`: Execution Time, Throughput, Speedup, Efficiency, CPU/Memory per Worker,
  jumlah task per Worker, load imbalance, serta daftar kegagalan beserta Recovery Time

## 8. Troubleshooting

| Gejala | Kemungkinan penyebab |
|---|---|
| Worker: `Gagal konek ke Master` | IP/port salah, firewall Master belum dibuka, beda jaringan, atau Wi-Fi memblokir antarperangkat |
| Master berhenti di `Menunggu minimal N Worker` | Jumlah Worker kurang. Turunkan `--min-workers` atau tambahkan Worker |
| Banyak status `TIMEOUT` | DNS publik membatasi query. Kurangi `--limit`, naikkan `--dns-timeout`, atau pakai resolver lokal |
| Speedup lebih kecil dari yang diharapkan | DNS bersifat I/O-bound dan bisa terbatas oleh jaringan atau resolver. Coba `--threads 4` atau `8`, dan jadikan bahan analisis |
| Hasil run ke-2 jauh lebih cepat | Cache resolver. Pakai subset domain berbeda per run atau rata-ratakan beberapa kali run |
| `ModuleNotFoundError: worker` di Master | `master.py` dan `worker.py` harus berada di folder yang sama |

## 9. Catatan etika

Gunakan jumlah domain yang wajar agar tidak membebani DNS publik, dan hindari menjalankan
pengujian skala besar berulang kali ke satu resolver yang sama.