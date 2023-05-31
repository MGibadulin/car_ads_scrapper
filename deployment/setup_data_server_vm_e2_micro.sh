
# sudo google_metadata_script_runner startup

# in case we haven't used that external disk previously, we need to format it
# sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb

if [ ! -d /mnt/disk-for-data ]; then
    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Mounting external data disk"
    echo
    sudo mkdir -p /mnt/disk-for-data
    echo UUID=`sudo blkid -s UUID -o value /dev/sdb` /mnt/disk-for-data/ ext4 discard,defaults,nofail 0 2 | sudo tee -a /etc/fstab
    sudo mount -o discard,defaults /dev/sdb /mnt/disk-for-data
    sudo chmod a+w /mnt/disk-for-data/
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Creating data folders"
    echo
    sudo mkdir -p /mnt/disk-for-data/mysql
    sudo mkdir -p /mnt/disk-for-data/car_ads_scrapper
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Installing docker subsystem"
    echo
    sudo apt update
    sudo apt install --yes docker.io
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Pulling mysql-server:8.0 docker image"
    echo
    sudo docker image pull mysql/mysql-server:8.0
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Starting mysql-server:8.0 docker container"
    echo
    sudo docker run --name mysql --restart=always -p 3306:3306 -v /mnt/disk-for-data/mysql:/var/lib/mysql/ -d -e "MYSQL_ROOT_PASSWORD=enter1" mysql/mysql-server:8.0
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Installing java"
    echo
    sudo apt install --yes openjdk-8-jre-headless
    export JAVA_HOME=/usr
    echo "JAVA_HOME=/usr" | sudo tee -a /etc/environment
    source /etc/environment

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Creating a swap file (1GB)"
    echo
    sudo mkdir -v /mnt/disk-for-data/swap
    cd /mnt/disk-for-data/swap
    sudo dd if=/dev/zero of=swapfile bs=1K count=1M

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Enabling swapping"
    echo
    sudo chmod 600 swapfile
    sudo mkswap swapfile
    sudo swapon swapfile

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Adding record to /etc/fstab for automounting swap file"
    echo
    echo "/mnt/disk-for-data/swap/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
    echo

    # set the rdbms up
    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Creating database (mysql) objects"
    echo
    sudo docker exec -i mysql mysql -uroot -penter1  < /soft/car_ads_scrapper/car_ads_db/DDL/create_objects.sql
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Creating database (mysql) users"
    echo
    sudo docker exec -i mysql mysql -uroot -penter1  < /soft/car_ads_scrapper/deployment/mysql_setup_users.sql
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Updating database (mysqld) settings"
    echo
    sudo docker exec -i mysql bash -c "cat > /etc/my.cnf" < /soft/car_ads_scrapper/deployment/mysql.cnf
    echo 
    echo

    # mysql --user=root --password="$(cat /root/.mysql)"

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Restaring rdbms (mysql) docker container"
    echo
    sudo docker restart mysql
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Installing mysql client tool"
    echo
    sudo apt install --yes mysql-client
    echo
    echo

    # sudo useradd external-user
    # sudo passwd external-user
    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Sharing /mnt/disk-for-data/car_ads_scrapper network folder "

    sudo apt install --yes nfs-kernel-server
    sudo systemctl enable nfs-server
    echo "/mnt/disk-for-data/car_ads_scrapper  spark-vm(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
    sudo exportfs -a
    sudo ufw allow 111
    sudo ufw allow 2049
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Cloning github repository"
    echo
    sudo mkdir /soft
    sudo git clone https://github.com/timoti1/car_ads_scrapper /soft/car_ads_scrapper
    echo
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Installing python dependencies"
    echo
    sudo apt install --yes python3-pip
    sudo pip3 install -r /soft/car_ads_scrapper/requirements.txt
    echo
    echo

    # sudo apt install --yes sudo iftop
else
    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "The automation script had been executed previously"
    echo 
    echo
fi

echo "------------------------------------------------------------"
echo $(date "+%Y-%m-%d %H:%M:%S") "Waiting for everything to be started and mounted"
echo
echo
# hope 1m is enough...
sleep 60

echo "------------------------------------------------------------"
echo $(date "+%Y-%m-%d %H:%M:%S") "Starting cards_finder_cars_com.py"
echo

cd /soft/car_ads_scrapper

# sudo nohup python3 cards_scrapper_cars_com.py &
# sudo python3 cards_finder_cars_com.py

#sleep 30
sudo python3  streamingETL-cars-com-to-BQ.py



