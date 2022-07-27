import os
import subprocess
from subprocess import PIPE, Popen
import time
import ruamel.yaml
from termcolor import colored
import argparse



LOGIN_PATH = "/home/steam1994"
TAG = "opensource-test"
SSH_KEY = "/home/steam1994/.ssh/id_rsa"
ssh_identity = '-i {}'.format(SSH_KEY) if SSH_KEY else ''
# Prefix for SSH and SCP.
SSH = 'ssh {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
SCP = 'scp -r {} -q -o ConnectTimeout=2 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '.format(
    ssh_identity)
USERNAME = "steam1994"
CMD_RETRY_TIMES = 3


def generate_ttcs_cfg_file(internal_ip, is_reference=False, use_ntp=False):
    if is_reference:
        content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 10
correct_clock: false'''
        cfg_file = content_str.replace("InternalIP", internal_ip)
        cfg_file_name = "ttcs-agent.cfg"
        with open(cfg_file_name, "w") as f:
            f.write(cfg_file)
        f.close()
        return cfg_file_name
    else:
        if use_ntp:
            content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 1
correct_clock: false'''
        else:
            content_str = '''management_address: "InternalIP"
log_dir: "/var/opt/ttcs/log"
subscription_mode: true
coordinator_address: "c-gjk1994gjk1994-c89e.gcp.clockwork.io"
coordinator_subscription_service_port: 6176
probe_address: "InternalIP"
clock_quality: 1
correct_clock: true'''
        cfg_file = content_str.replace("InternalIP", internal_ip)
        cfg_file_name = "ttcs-agent.cfg"
        with open(cfg_file_name, "w") as f:
            f.write(cfg_file)
        f.close()
        return cfg_file_name




def retry_proc_error(procs_list):
    procs_error = []
    for server, proc, cmd in procs_list:
        output, err = proc.communicate()
        if proc.returncode != 0:
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
            procs_error.append((server, proc, cmd))
    return procs_error


def start_ttcs_node(internal_ip, is_reference, use_ntp=False):
    clean_prev_deb_cmd = "sudo dpkg -P ttcs-agent"
    run_command([internal_ip], clean_prev_deb_cmd, in_background=False)
    install_deb_cmd = "sudo dpkg -i /home/steam1994/ttcs-agent_1.0.21_amd64.deb"
    #install_deb_cmd = "sudo dpkg -i /root/ttcs-agent_1.0.12_amd64.deb"
    run_command([internal_ip], install_deb_cmd, in_background=False)

    cfg_file = generate_ttcs_cfg_file(internal_ip, is_reference, use_ntp)
    local_file_path = "./ttcs-agent.cfg"
    remote_dir = "/etc/opt/ttcs"
    remote_path = remote_dir + "/ttcs-agent.cfg"

    chmod_cmd = "sudo chmod -R 777 {remote_dir}".format(remote_dir=remote_dir)
    run_command([internal_ip], chmod_cmd, in_background=False)

    rm_cmd = "sudo rm -f {remote_path}".format(remote_path=remote_path)
    run_command([internal_ip], rm_cmd, in_background=False)

    scp_files([internal_ip], local_file_path, remote_path, to_remote=True)

    if is_reference is not True and use_ntp is False:
        stop_ntp_cmd = "sudo systemctl stop ntp"
        run_command([internal_ip], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable ntp"
        run_command([internal_ip], disable_ntp_cmd, in_background=False)
        stop_ntp_cmd = "sudo systemctl stop chronyd"
        run_command([internal_ip], stop_ntp_cmd, in_background=False)
        disable_ntp_cmd = "sudo systemctl disable chronyd"
        run_command([internal_ip], disable_ntp_cmd, in_background=False)
    else:
        enable_ntp_cmd = "sudo systemctl enable chronyd"
        run_command([internal_ip], enable_ntp_cmd, in_background=False)
        start_ntp_cmd = "sudo systemctl start chronyd"
        run_command([internal_ip], start_ntp_cmd, in_background=False)

    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command([internal_ip], sys_start_ttcp_agent_cmd, in_background=False)


def launch_ttcs(server_ip_list):
    stop_ntp_cmd = "sudo systemctl stop chronyd"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable chronyd"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    stop_ntp_cmd = "sudo systemctl stop ntp"
    run_command(server_ip_list, stop_ntp_cmd, in_background=False)
    disable_ntp_cmd = "sudo systemctl disable ntp"
    run_command(server_ip_list, disable_ntp_cmd, in_background=False)
    sys_start_ttcp_agent_cmd = "sudo systemctl start ttcs-agent"
    run_command(server_ip_list, sys_start_ttcp_agent_cmd, in_background=False)



def scp_files(server_ip_list, local_path_to_file, remote_dir, to_remote):
    '''
    copies the file in 'local_path_to_file' to the 'remote_dir' in all servers
    whose external ip addresses are in 'server_ip_list'

    args
        server_ip_list: list of external IP addresses to communicate with
        local_path_to_file: e.g. ./script.py
        remote_dir: e.g. ~
        to_remote: whether to copy to remote (true) or vice versa (false)
    returns
        boolean whether operation was succesful on all servers or not
    '''
    src = remote_dir if not to_remote else local_path_to_file
    src_loc = 'remote' if not to_remote else 'local'
    dst = remote_dir if to_remote else local_path_to_file
    dst_loc = 'remote' if to_remote else 'local'

    message = 'from ({src_loc}) {src} to ({dst_loc}) {dst}'.format(
        src_loc=src_loc, src=src, dst_loc=dst_loc, dst=dst)
    print('---- started scp {}'.format(message))

    procs = []
    for server in server_ip_list:
        if to_remote:
            cmd = '{} {} {}@{}:{}'.format(SCP, local_path_to_file,
                                          USERNAME, server, remote_dir)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        else:
            cmd = '{} {}@{}:{} {}'.format(SCP, USERNAME, server,
                                          remote_dir, local_path_to_file)
            proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        # print("scp cmd ", cmd)
        procs.append((server, proc, cmd))

    success = True
    procs_error = retry_proc_error(procs)
    retries = 1
    while retries < CMD_RETRY_TIMES and procs_error:
        procs_error = retry_proc_error(procs)
        retries += 1

    if retries >= CMD_RETRY_TIMES and procs_error:
        success = False
        for server, proc, cmd in procs_error:
            output, err = proc.communicate()
            if proc.returncode != 0:
                print(
                    colored('[{}]: FAIL SCP - [{}]'.format(server, cmd),
                            'yellow'))
                print(colored('Error Response:', 'blue', attrs=['bold']),
                      proc.returncode, output, err)

    if success:
        print(
            colored('---- SUCCESS SCP {} on {}'.format(message,
                                                       str(server_ip_list)),
                    'green',
                    attrs=['bold']))
    else:
        print(
            colored('---- FAIL SCP {}'.format(message), 'red', attrs=['bold']))
    return success


def run_command(server_ip_list, cmd, in_background=True):
    '''
    runs the command 'cmd' in all servers whose external ip addresses are 
    in 'server_ip_list'

    cfg
        server_ip_list: list of external IP addresses to communicate with
        cmd: command to run
    returns
        boolean whether operation was succesful on all servers or not
    '''
    if not in_background:
        print('---- started to run command - [{}] on {}'.format(
            cmd, str(server_ip_list)))
    else:
        print(
            colored('---- started to run [IN BACKGROUND] command - [{}] on {}'.
                    format(cmd, str(server_ip_list)),
                    'blue',
                    attrs=['bold']))
    procs = []
    for server in server_ip_list:
        ssh_cmd = '{} {}@{} {}'.format(SSH, USERNAME, server, cmd)
        proc = Popen(ssh_cmd.split(), stdout=PIPE, stderr=PIPE)
        procs.append((server, proc, ssh_cmd))

    success = True
    output = ''
    if not in_background:
        procs_error = retry_proc_error(procs)
        retries = 1
        while retries < CMD_RETRY_TIMES and procs_error:
            procs_error = retry_proc_error(procs)
            retries += 1

        if retries >= CMD_RETRY_TIMES and procs_error:
            success = False
            for server, proc, cmd in procs_error:
                output, err = proc.communicate()
                if proc.returncode != 0:
                    print(
                        colored(
                            '[{}]: FAIL run command - [{}]'.format(
                                server, cmd), 'yellow'))
                    print(colored('Error Response:', 'blue', attrs=['bold']),
                          proc.returncode, output, err)

        if success:
            print(
                colored('---- SUCCESS run command - [{}] on {}'.format(
                    cmd, str(server_ip_list)),
                        'green',
                        attrs=['bold']))
        else:
            print(
                colored('---- FAIL run command - [{}]'.format(cmd),
                        'red',
                        attrs=['bold']))

    return success, output





def create_instance(instance_name,
                    image=None,
                    machine_type = "n1-standard-4",
                    customzedZone = "us-central1-a",
                    customzedIp = None,
                    require_external_ip=False,
                    second_ip = False
                    ):
    # Construct gcloud command to create instance.
    

    network_address_config = ("--network-interface no-address"
                              if require_external_ip == False else "")
    
    if customzedIp is not None:
        network_address_config += ",private-network-ip="+customzedIp
        
    if second_ip:
        network_address_config += " --network-interface subnet=subnet-1,no-address"
    # scopes = "--scopes storage-full,https://www.googleapis.com/auth/bigtable.admin,https://www.googleapis.com/auth/bigtable.data,https://www.googleapis.com/auth/bigquery"
    # if full_access_to_cloud_apis:
    scopes = "--scopes=https://www.googleapis.com/auth/cloud-platform"

    create_instance_cmd = """gcloud beta compute instances create {inst} --zone {zone} --image-family {source_image} --machine-type {machine_type} {network} {scopes} --boot-disk-size 50GB""".format(
        inst=instance_name,
        zone=customzedZone,
        source_image=image,
        machine_type=machine_type,
        network=network_address_config,
        scopes=scopes,
    )

    # print(create_instance_cmd)
    # Run gcloud command to create machine.
    proc = Popen(create_instance_cmd, stdout=PIPE, stderr=PIPE, shell=True)
    # Wait for the process end and print error in case of failure
    output, error = proc.communicate()
    if proc.returncode != 0:
        print(colored("Failed to create instance", color="red",
                      attrs=["bold"]))
        print(colored("Error Response: ", color="blue", attrs=["bold"]),
              output, error)



def del_instance_list(instance_list, zone="us-central1-a"):
    for machine in instance_list:
        print(colored("Deleting "+machine, "red", attrs=['bold']))
        subprocess.Popen(
            'gcloud -q compute instances delete {inst} --zone {zone}'.format(
                inst=machine, zone=zone).split())

def stop_instance_list(instance_list, zone="us-central1-a"):
    stop_cmd = 'gcloud compute instances stop {inst} --zone {zone}'.format(
            inst=' '.join(instance_list), zone = zone
            )
    print(stop_cmd)
    os.system(stop_cmd)


def start_instance_list(instance_list, zone="us-central1-a"):
    start_cmd = 'gcloud compute instances start {inst} --zone {zone}'.format(
            inst=' '.join(instance_list), zone = zone
            )
    print(start_cmd)
    os.system(start_cmd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_replicas',  type=int, default = 3,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_proxies',  type=int, default = 2,
                        help='Specify the number of proxies ')
    parser.add_argument('--num_clients',  type=int, default = 10,
                        help='Specify the number of clients ')
    args = parser.parse_args()

    num_replicas = args.num_replicas
    num_proxies = args.num_proxies
    num_clients = args.num_clients
    print("replicas: ", num_replicas)
    print("proxies: ", num_proxies)
    print("clients: ", num_clients)

    
    # cfg_file_name = generate_ttcs_cfg_file("10.128.3.79", is_reference=True, use_ntp=False)
    
    replica_ips = ["10.128.2."+str(i+10) for i in range(10)]
    proxy_ips = ["10.128.2."+str(i+20) for i in range(10) ]
    client_ips = ["10.128.2."+str(i+30) for i in range(100) ]

    replica_ips = replica_ips[0:num_replicas]
    proxy_ips = proxy_ips[0:num_proxies]
    client_ips = client_ips[0:num_clients]

    replica_name_list = [TAG+"-replica-"+str(i) for i in range(num_replicas) ]
    proxy_name_list = [ TAG+"-proxy-"+str(i) for i in range(num_proxies) ]
    client_name_list = [ TAG+"-client-"+str(i) for i in range(num_clients) ]

    vm_ips = replica_ips + proxy_ips + client_ips
    vm_name_list = replica_name_list + proxy_name_list + client_name_list

    replica_vm_type = "n1-standard-16"
    proxy_vm_type = "n1-standard-32"
    client_vm_type = "n1-standard-4"

    binary_path = "{login_path}/nezhav2/bazel-bin/".format(login_path = LOGIN_PATH)

    config_path = "{login_path}/nezhav2/configs".format(login_path = LOGIN_PATH)

    yaml = ruamel.yaml.YAML()


    # for i in range(num_replicas):
    #     create_instance(instance_name = replica_name_list[i],
    #                     image= "opensource-nezha",
    #                     machine_type =  replica_vm_type,
    #                     customzedZone="us-central1-a",
    #                     customzedIp = replica_ips[i] )
    #     print(colored("Created "+replica_name_list[i], "green", attrs=['bold']))
        
    # exit(0)

    # for i in range(num_proxies):
    #     create_instance(instance_name = proxy_name_list[i],
    #                     image= "opensource-nezha",
    #                     machine_type =  proxy_vm_type,
    #                     customzedZone="us-central1-a",
    #                     customzedIp = proxy_ips[i] )
    #     print(colored("Created "+proxy_name_list[i], "green", attrs=['bold']))
        

    # for i in range(num_clients):
    #     create_instance(instance_name = client_name_list[i],
    #                     image= "opensource-nezha",
    #                     machine_type =  client_vm_type,
    #                     customzedZone="us-central1-a",
    #                     customzedIp = client_ips[i] )
    #     print(colored("Created "+client_name_list[i], "green", attrs=['bold']))


    # time.sleep(120)
    # for i in range(len(vm_ips)):
    #     start_ttcs_node(vm_ips[i],False)
    # exit(0)

    #### del_instance_list(instance_list=vm_name_list)


    # stop_instance_list(instance_list = vm_name_list)
    # exit(0)


    # start_instance_list(instance_list = vm_name_list)
    # time.sleep(60)
    # print(vm_ips)
    # launch_ttcs(vm_ips)
    # exit(0)

    # start_ttcs_node(replica_ips[3],False)
    # exit(0)

    test_no = 1
    enable_dom =1
    # enable_dom = 1
    #poisson_rate = 10000
    poisson_rate = 5000
    percentile = 50
    while len(replica_ips) < 5:
        replica_ips += ["127.0.0.1"]
    print(replica_ips)
    for test_no in range(1,6):
        for percentile in [50]: #[50,75,90,95]:
            remote_path = "{login_path}/nezhav2/bazel-bin/*".format(login_path = LOGIN_PATH)
            rm_cmd = "sudo rm -rf {remote_path}".format(remote_path=remote_path)
            run_command(vm_ips, rm_cmd, in_background=False)

            mkdir_cmd = "mkdir -p {binary_path}/micro-bench".format(binary_path = binary_path)
            run_command(vm_ips, mkdir_cmd, in_background=False)

            
            binary_file = "{binary_path}/micro-bench/bench_sender".format(binary_path=binary_path)
            scp_files(vm_ips, binary_file, binary_file, to_remote = True)

            binary_file = "{binary_path}/micro-bench/bench_receiver".format(binary_path=binary_path)
            scp_files(vm_ips, binary_file, binary_file, to_remote = True)

            # Kill existing procs
            kill_cmd = "sudo pkill -9 bench_receiver"
            run_command(vm_ips, kill_cmd, in_background=False)
            kill_cmd = "sudo pkill -9 bench_sender"
            run_command(vm_ips, kill_cmd, in_background=False)

            rm_cmd = "sudo rm -rf Replica-Stats*.csv"
            run_command(vm_ips, rm_cmd, in_background=False)



            ## Launch replicas (id starts from 0)
            for i in range(num_replicas):
                replica_cmd = "{binary_path}/micro-bench/bench_receiver --receiver_ip {ip} --replica_id {id} --enable_dom {enable_dom} --percentile {percentile} >{log_file} 2>&1 &".format(
                binary_path = binary_path,
                ip = replica_ips[i],
                id = i,
                enable_dom = enable_dom,
                percentile = percentile,
                log_file = "receiver-log-"+str(i)
                )
                print(colored(replica_cmd, "yellow", attrs=['bold']))
                run_command([replica_ips[i]], replica_cmd, in_background=False)



            # Launch clients (id starts from 2)
            for i in range(num_clients):
                client_cmd = "{binary_path}/micro-bench/bench_sender --receiver_1_ip {ip1} --receiver_2_ip {ip2} --receiver_3_ip {ip3} --receiver_4_ip {ip4} --receiver_5_ip {ip5} --receiver_num {receiver_num} --client_ip {myip} --poisson_rate {poisson_rate} --client_id {id}   >{log_file} 2>&1 &".format(
                    binary_path = binary_path,
                    ip1 = replica_ips[0], 
                    ip2 = replica_ips[1],
                    ip3 = replica_ips[2], 
                    ip4 = replica_ips[3],
                    ip5 = replica_ips[4], 
                    receiver_num = num_replicas,
                    myip = client_ips[i],
                    poisson_rate = poisson_rate,
                    id =  i+1,
                    log_file = "client-log-"+str(i+1) 
                ) 
                print(colored(client_cmd, "yellow", attrs=['bold']))
                run_command([client_ips[i]], client_cmd, in_background = True)
                
            # exit(0)
            print("Sleep...")
            time.sleep(90)

            # Copy Stats File
            folder_name = "micro-stats"
            sub_folder_name = "T-{test_no}-{num_replicas}-{num_clients}-{poisson_rate}-{enable_dom}-{percentile}".format(
                test_no = test_no,
                num_replicas  = num_replicas,
                num_clients = num_clients, 
                poisson_rate = poisson_rate, 
                enable_dom = enable_dom,
                percentile = percentile
            )
            stats_folder = "{login_path}/{folder_name}/{sub_folder_name}".format(
                login_path = LOGIN_PATH,
                folder_name = folder_name,
                sub_folder_name = sub_folder_name
            )
            mkdir_cmd = "sudo mkdir -p -m 777 {stats_folder}".format(stats_folder = stats_folder)
            os.system(mkdir_cmd)


            for i in range(num_replicas):
                file_name = "Replica-Stats-"+str(i)+".csv"
                local_file_path = "{stats_folder}/{file_name}".format(
                    stats_folder = stats_folder,
                    file_name = file_name
                )
                remote_path = "{stats_folder}/{file_name}".format(
                    stats_folder = LOGIN_PATH,
                    file_name = file_name
                )
                scp_files([replica_ips[i]], local_file_path, remote_path, to_remote=False)