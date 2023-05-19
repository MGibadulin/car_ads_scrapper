
# sudo google_metadata_script_runner startup

# in case we haven't used that external disk previously, we need to format it
# sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb

if [ ! -d /mnt/disk-for-data ]; then
    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Mounting external data disk" $(echo )
    echo
    sudo mkdir -p /mnt/disk-for-data
    echo UUID=`sudo blkid -s UUID -o value /dev/sdb` /mnt/disk-for-data/ ext4 discard,defaults,nofail 0 2 | sudo tee -a /etc/fstab
    sudo mount -o discard,defaults /dev/sdb /mnt/disk-for-data
    sudo chmod a+w /mnt/disk-for-data/
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Creating data folders"
    sudo mkdir -p /mnt/disk-for-data/mysql
    sudo mkdir -p /mnt/disk-for-data/car_ads_scrapper
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Installing docker subsystem"
    sudo apt update
    sudo apt install --yes docker.io
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Pulling mysql-server:8.0 docker image"
    sudo docker image pull mysql/mysql-server:8.0
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Staarting mysql-server:8.0 docker container"
    sudo docker run --name mysql --restart=always -p 3306:3306 -v /mnt/disk-for-data/mysql:/var/lib/mysql/ -d -e "MYSQL_ROOT_PASSWORD=enter1" mysql/mysql-server:8.0
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Cloning github repository"
    sudo mkdir /soft
    sudo git clone https://github.com/timoti1/car_ads_scrapper /soft/car_ads_scrapper
    echo 
    echo

    #sudo apt install --yes openjdk-8-jre-headless
    #export JAVA_HOME=/usr/bin/java              #$(which java)
    #echo "JAVA_HOME=/usr/bin/java" >> /etc/environment

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Installing python dependencies"
    sudo apt install --yes python3-pip
    sudo pip3 install -r /soft/car_ads_scrapper/requirements_scrapper.txt
    echo 
    echo

    # set the rdbms up
    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Creating database (mysql) objects"
    sudo docker exec -i mysql mysql -uroot -penter1  < /soft/car_ads_scrapper/car_ads_db/DDL/create_objects.sql
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Creating database (mysql) users"
    sudo docker exec -i mysql mysql -uroot -penter1  < /soft/car_ads_scrapper/mysql_setup_users.sql
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Updating database (mysqld) settings"
    sudo docker exec -i mysql /bash -c "cat > /etc/my.cnf" < /soft/car_ads_scrapper/deployment/mysql.cnf
    echo 
    echo

    # mysql --user=root --password="$(cat /root/.mysql)"

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Restaring rdbms (mysql) docker container"
    sudo docker restart mysql
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Installing mysql client tool"
    sudo apt install --yes mysql-client
    echo "------------------------------------------------------------"
    echo

    echo "------------------------------------------------------------"
    echo $(date "%X UTC:   ") "Creating a swap file (2GB)"
    sudo mkdir -v /var/cache/swap
    cd /var/cache/swap
    sudo dd if=/dev/zero of=swapfile bs=1K count=2M

    echo $(date "%X UTC:   ") "Enabling swapping"
    sudo chmod 600 swapfile
    sudo mkswap swapfile
    sudo swapon swapfile

    echo $(date "%X UTC:   ") "Adding record to /etc/fstab for automounting swap file"
    echo "/var/cache/swap/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
    echo "------------------------------------------------------------"
    echo
else
    echo 
    echo $(date "%X UTC:   ") "The automation script had been executed previously"
    echo 
    echo
fi

echo 
echo $(date "%X UTC:   ") "Waiting for everything to be started and mounted"
# hope 1m is enough...
sleep 60
echo "------------------------------------------------------------"
echo

echo "------------------------------------------------------------"
echo $(date "%X UTC:   ") "Starting cards_finder_cars_com.py"
cd /soft/car_ads_scrapper
sudo nohup python3 cards_finder_cars_com.py &
echo "------------------------------------------------------------"
echo

# sleep 30
# sudo nohup python3 cards_scrapper_cars_com.py &



