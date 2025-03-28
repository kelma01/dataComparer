# dataComparer

Setup:

```bash
git clone project_url.git
cd project_folder/
python -m venv venv
pip install -r requirements.txt
#her reader dosyasının comment'inde nasıl çalıştırılacağı yer almaktadır
```

HDFS Setup:

1. WSL veya bir Linux sistemi kurun.
2. `sudo apt update && sudo apt install openjdk-11-jdk -y` Java 11 kuruluum gereklidir.
3. `cd ~` ve `wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz` ile hadoop indirilir.
4. `tar -xvzf hadoop-3.3.6.tar.gz` ile dosyalar boşaltılır.
5. `sudo mv hadoop-3.3.6 /usr/local/hadoop` klasör taşıma.
6. `nano ~/.bashrc` ile terminale gidilip aşağıdaki satırlar sona eklenir.
    ```bash
    export HADOOP_HOME=/usr/local/hadoop
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    export PATH=$PATH:$JAVA_HOME/bin
    ```
7. `source ~/.bashrc` ile aktive edilir.
8. `sudo apt update && sudo apt install openssh-server -y` ile ssh kontrol edilir sonrasında ssh servisi başlatılır. `sudo service ssh start`
9. Ardından belirtilen dosyalara belirtilen scriptler sona eklenir.
    ```bash
    nano $HADOOP_HOME/etc/hadoop/core-site.xml
    ########################################
    <configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    </configuration>



    nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
    ########################################
    <configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///usr/local/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///usr/local/hadoop/data/datanode</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    </configuration>




    nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
    ##########################################
    <configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    </configuration>




    nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
    ########################################
    <configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    </configuration>

    ```

10. `ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa` ile SSH anahtarı oluşturulur ve deavmında auth_key olarak eklenir. `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys` 
11. `sudo service ssh start` ile ssh servisi başlatılıp `ssh localhost` ile ssh testi yapılabilir.
12. İndirilen java versionuna geçmek için `sudo update-alternatives --config java` çalıştırlır ve 11. sürüm seçilir. 
13. `nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh` dosyasını açıp export JAVA_HOME ile başlayan satırı aşağıdaki ile değiştir:
    ```bash
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    ```
14. `nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh` dosyasını açıp export HADOOP_HOME ile başlayan satırı aşağıdaki ile değiştir:
    ```bash
    export HADOOP_HOME=/usr/local/hadoop
    ```
15. Tekrardan `source ~/.bashrc` ile aktive edip `start-dfs.sh` ile başlat. (`stop-dfs.sh` kapatır.)(`jps` ile çalışıp çalışmadığı görülebilir.)

 

Proje için yapılacaklar:
1. storage için hdfs kurulumu
2. şu fb aldığımız avro ve parquet sıkıştırma olayları
3. 