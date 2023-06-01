
# sudo google_metadata_script_runner startup

# in case we haven't used that external disk previously, we need to format it
# sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb

if [ ! -f /soft/car_ads_scrapper/scrapping-vm-configured ]; then
    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Creating data folders"
    echo
    sudo mkdir -p /mnt/disk-for-data/car_ads_scrapper
    echo 
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Mounting car_ads_scrapper network folder "
    sudo apt update
    sudo apt install --yes nfs-common
    sudo mount data-server-vm:/mnt/disk-for-data/car_ads_scrapper /mnt/disk-for-data/car_ads_scrapper
    echo "data-server-vm:/mnt/disk-for-data/car_ads_scrapper /mnt/disk-for-data/car_ads_scrapper nfs defaults 0 0" | sudo tee -a /etc/fstab
    echo


    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Creating a swap file (1GB)"
    echo
    sudo mkdir -v /var/cache/swap
    cd /var/cache/swap
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
    echo "/var/cache/swap/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
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
    sudo pip3 install -r /soft/car_ads_scrapper/requirements_scrapper.txt
    echo
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Waiting for everything to be started and mounted"
    echo
    echo
    # hope 1m is enough...
    sleep 60

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Create scrapping-vm-configured file as a sign of the installation completed"
    echo
    cd /soft/car_ads_scrapper
    sudo touch /soft/car_ads_scrapper/scrapping-vm-configured
    echo

    echo "------------------------------------------------------------"
    echo $(date "+%Y-%m-%d %H:%M:%S") "Starting your application"
    echo
    cd /soft/car_ads_scrapper

    sudo python3 cards_scrapper_cars_com.py
    # sudo python3 cards_finder_cars_com.py
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

#echo "------------------------------------------------------------"
#echo $(date "+%Y-%m-%d %H:%M:%S") "Starting your application"
#echo
#
#cd /soft/car_ads_scrapper
#
## sudo nohup python3 cards_scrapper_cars_com.py &
## sudo python3 cards_finder_cars_com.py
#
##sleep 30
#sudo python3  streamingETL-cars-com-to-BQ.py
#
#

