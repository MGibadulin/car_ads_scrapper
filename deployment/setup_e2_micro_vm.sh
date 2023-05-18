
# sudo google_metadata_script_runner startup

# sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb
sudo mkdir -p /mnt/disk-for-data

echo UUID=`sudo blkid -s UUID -o value /dev/sdb` /mnt/disk-for-data/ ext4 discard,defaults,nofail 0 2 | sudo tee -a /etc/fstab

sudo mount -o discard,defaults /dev/sdb /mnt/disk-for-data
sudo chmod a+w /mnt/disk-for-data/

sudo mkdir -p /mnt/disk-for-data/mysql
sudo mkdir -p /mnt/disk-for-data/car_ads_scrapper

sudo apt update	

sudo apt install --yes docker.io
sudo docker image pull mysql/mysql-server:8.0
sudo docker run --name mysql --restart=always -p 3306:3306 -v /mnt/disk-for-data/mysql:/var/lib/mysql/ -d -e "MYSQL_ROOT_PASSWORD=enter1" mysql/mysql-server:8.0

sudo mkdir /soft
git clone https://github.com/timoti1/car_ads_scrapper /soft/car_ads_scrapper

#sudo apt install --yes openjdk-8-jre-headless
#export JAVA_HOME=/usr/bin/java              #$(which java)
#echo "JAVA_HOME=/usr/bin/java" >> /etc/environment

apt install --yes python3-pip
pip3 install -r /soft/car_ads_scrapper/requirements_scrapper.txt

# set the rdbms up
sudo docker exec -i mysql bash  <<< "echo 'bind-address  = 0.0.0.0' >> /etc/my.cnf"
sudo docker exec -i mysql mysql -uroot -penter1  <<< "create user 'timoti'@'%' identified by 'enter1'; grant all privileges on *.* to 'timoti'@'%';"
sudo docker exec -i mysql mysql -uroot -penter1  <<< "create user 'root'@'%' identified by 'enter1'; grant all privileges on *.* to 'root'@'%';"
sudo docker restart mysql

sudo apt install --yes mysql-client



# prepare the db to work with
mysql -h 127.0.0.1 -utimoti -penter1 < /soft/car_ads_scrapper/car_ads_db/DDL/create_objects.sql


cd /soft/car_ads_scrapper
nohup python3 cards_finder_cars_com.py &


